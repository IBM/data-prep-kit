# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import logging
import os
import re
from os.path import normpath
from typing import Callable, List, Tuple

import mkdocs.plugins


log = logging.getLogger("mkdocs")

GITHUB_REPO = os.getenv("REPO_URL", "https://github.com/IBM/data-prep-lab")
GITHUB_BRANCH = os.getenv("REPO_BRANCH", "dev")
GITHUB_VIEW_FILE = "blob"
GITHUB_VIEW_TREE = "tree"

NORMAL_LINK = r"(?<!\!)\[([^\[]+)\]\(([^)]+)\)"


def get_all_links(text: str) -> List[Tuple[str, str]]:
    """
    This function extracts the links from markdown text.

    Args:
        text: markdown text

    Returns:
        A list of links as tuples (text, link)
    """
    pattern = NORMAL_LINK
    match = re.findall(pattern, text)
    return match


def update_link(markdown, text, old_link, new_link):
    """Updates the link with updated value

    Args:
        markdown: The markdown text to modify.
        text: The text which is linked. i.e [text](link)
        old_link: The link to be replaced.
        new_link: The new link to insert.

    Returns:
        The updated markdown text with the updated link.
    """

    pattern = r"\[" + text + r"\]\(" + old_link + "\)"
    return re.sub(pattern, r"[" + text + r"](" + new_link + ")", markdown)


def update_markdown_content_updated(markdown: str, replacements: List[Tuple[str, str, str]]) -> str:
    """
    This function updates markdown content by replacing old links with updated links.

    Args:
        markdown: markdown text as a string

        replacements: A list of Tuples of the form (text, old_link, updated_link)

    Returns:
        Updated markdown text as a string
    """
    if len(replacements) > 0:
        for text, old_value, new_value in replacements:
            log.info(f"Updating Link: text: [{text}], link: {old_value}, updated link: {new_value}")
            markdown = update_link(markdown, text, old_value, new_value)
    return markdown


@mkdocs.plugins.event_priority(-50)
def on_page_markdown(markdown, page, **kwargs):
    path = page.file.src_uri

    log.info(f"Processing {path}.")

    def get_repo_relative_path(link: str) -> str:
        """
        This function computes the repository relative path of the link.

        Args:
            link: relative path to a file or a folder

        Returns:
            Relative Path of the link wrt repo root folder
        """
        base_name = os.path.basename(path)
        p_path = path.replace(base_name, "")
        check_path = os.path.join(p_path, link)
        return check_path

    def is_folder(link: str) -> bool:
        """
        This function detects whether the link points to a folder.

        Args:
            link: relative path to a file or a folder

        Returns:
            True, if the link refers to a folder. False otherwise.
        """
        relative_path = get_repo_relative_path(link)
        return os.path.isdir(relative_path)

    def is_file(link: str) -> bool:
        """
        This function detects whether the link points to a file.

        Args:
            link: relative path to a file or a folder

        Returns:
            True, if the link refers to a file. False otherwise.
        """
        relative_path = get_repo_relative_path(link)
        return os.path.isfile(relative_path)

    def is_file_of_type(link, file_extension):
        """
        This function detects whether the link points to a file of particular type

        Args:
            link: relative path to a file or a folder
            file_extension: represents file type like .py for python .md for markdown
        Returns:
            True, if the file has required file extension. False otherwise.
        """
        path, extension = os.path.splitext(link)
        if extension == file_extension:
            return True
        else:
            return False

    def relative_path_exists(link: str) -> bool:
        """
        This function detects whether the location pointed by link exists on the filesystem.

        Args:
            link: relative path to a file or a folder

        Returns:
            True, if the path exists. False otherwise.
        """
        relative_path = get_repo_relative_path(link)
        return os.path.exists(relative_path)

    def link_updater_func(
        view: str,
    ) -> Callable[[Tuple[str, str]], Tuple[str, str, str]]:
        """
        This function detects whether the location pointed by link exists on the filesystem.

        Args:
            view: repository view referrring to a folder or a file.
                  values are GITHUB_VIEW_TREE or GITHUB_VIEW_BLOB

        Returns:
            A function which can update links tuple with updated links.
        """

        def update_link(pair: Tuple[str, str]) -> Tuple[str, str, str]:
            link = normpath(get_repo_relative_path(pair[1]))
            prefix = os.path.join(os.path.join(os.path.join(GITHUB_REPO, view), GITHUB_BRANCH), link)
            return (pair[0], pair[1], prefix)

        return update_link

    # Extract all markdown links, we select links of format `[text](link)`
    # Extracted links don't include image links formatted as `![alt text](image.jpg)`)
    # It returns a list of tuples like [('text', 'link'), ]
    all_links = get_all_links(markdown)

    # Filter only the links which refer to local files/folders
    # in the repo. This step ensures we don't include `http`, `ssh` or other protocol links
    path_links = list(filter(lambda link_tuple: relative_path_exists(link_tuple[1]), all_links))

    # Since mkdocs can render markdown files, leave out links to markdown files
    path_links = list(filter(lambda link_tuple: not is_file_of_type(link_tuple[1], ".md"), path_links))

    replacement_tuples = []
    for selector, view in [(is_folder, GITHUB_VIEW_TREE), (is_file, GITHUB_VIEW_FILE)]:
        # Filter the links of local files or folders, based on selector
        selected = list(filter(lambda link_tuple: selector(link_tuple[1]), path_links))

        # Prepare tuple (link_text, old_link, updated_link). This tuple would be used to replace `old_link` with `updated_link`
        # `old_link` is a relative link to local directory/folder in the repo
        # `updated_link` is a github repo link to the directory/folder in the repo on GITHUB_BRANCH
        replacement_tuples = replacement_tuples + list(map(link_updater_func(view), selected))

    if len(replacement_tuples) > 0:
        # Update markdown content using replacement tuples
        # old_links which are relative to the repo are replaced with
        # github repo links to the files.
        return update_markdown_content_updated(markdown, replacement_tuples)
