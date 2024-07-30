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
from html.parser import HTMLParser

import requests


class PackageListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.packages = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr in attrs:
                if attr[0] == "href":
                    # Extract package name from the href attribute
                    package_name = attr[1].split("/")[2]  # Adjusted index to correctly extract the package name
                    self.packages.append(package_name)


def fetch_packages(url="https://pypi.org/simple/"):
    response = requests.get(url)
    response.raise_for_status()  # This will raise an error for bad requests

    parser = PackageListParser()
    parser.feed(response.text)
    return parser.packages


def save_packages_to_file(packages, filename="pypi_packages.txt"):
    with open(filename, "w") as file:
        for package in sorted(packages):  # Sorting the package list before saving
            file.write(f"{package}\n")
    print(f"Stored {len(packages)} packages to {filename}")


# This sets an absolute path to the filename
def check_package_exists(package_name):
    filename = "pypi_packages.txt"
    try:
        with open(filename, "r") as file:
            package_set = set(line.strip() for line in file)
    except FileNotFoundError:
        print(f"Package list file not found at {filename}. Please make sure to fetch the list first.")
        return False
    return package_name in package_set


# Fetch and save packages
# packages = fetch_packages()
# save_packages_to_file(packages)

module_name = "numpy"
if check_package_exists(module_name):
    print(f"Package '{module_name}' exists")
