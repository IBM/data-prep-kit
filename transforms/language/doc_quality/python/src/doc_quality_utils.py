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

import os
import time
from typing import Set


SUPPORT_LANGUAGES = ["de", "en", "es", "fr", "ja", "pt"]


def load_bad_words(ft_lang: str, file_path: str) -> Set[str]:
    """
    Load list of bad words from file, eg
    load_bad_words(ft_lang="en",file_path="/tmp/ldnoobw/en")
    """
    bad_words = []
    if ft_lang not in SUPPORT_LANGUAGES:
        print(f"== Not support ldnoobw for {ft_lang} yet!")
        return bad_words

    file_path = os.path.expanduser(file_path)

    # Check if the file exists before attempting to read it
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                bad_words = list(set(ln.strip() for ln in f.readlines()))
        except Exception as e:
            raise Exception(f"== Failed in reading ldnoobw for {ft_lang} from {file_path}")
    else:
        raise Exception(f"== {file_path} is invalid")

    return bad_words


def get_time(include_date=True):
    from datetime import datetime, timezone

    utc_now = datetime.now(timezone.utc)
    local_now = utc_now.astimezone()
    if include_date is True:
        current_time = local_now.strftime("%Y-%m-%d %H:%M:%S")
    else:
        current_time = local_now.strftime("%H:%M:%S")
    return f"{current_time}"


def contain_search_pattern(args) -> float:
    pattern_search, chunk = args
    verbose = False
    if verbose:
        print(
            f"== {get_time(False)}: {os.getpid()} pattern_search: {pattern_search} len(chunk): {len(chunk):,} ",
            flush=True,
        )
        time.sleep(1)
    return 1.0 if pattern_search in chunk else 0.0
