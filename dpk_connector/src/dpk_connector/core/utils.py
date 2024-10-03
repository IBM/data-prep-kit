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

import re
from urllib.parse import ParseResult, urlparse

import tldextract
from scrapy.http import Request, Response
from scrapy.http.headers import Headers
from scrapy.utils.httpobj import urlparse_cached as _urlparse_cached


def _get_header_value(headers: Headers, key: str) -> str | None:
    value = headers.get(key)
    return value.decode("utf-8") if value else None


def get_header_value(response: Response, key: str) -> str | None:
    return _get_header_value(response.headers, key)


def get_content_type(response: Response) -> str | None:
    return get_header_value(response, "Content-Type")


def get_mime_type(content_type: str) -> str:
    return content_type.split(";")[0].strip()


def urlparse_cached(input: str | Request | Response) -> ParseResult:
    return urlparse(input) if isinstance(input, str) else _urlparse_cached(input)


def get_netloc(input: str | Request | Response) -> str:
    return urlparse_cached(input).netloc


def get_base_url(input: str | Request | Response | ParseResult) -> str:
    if isinstance(input, ParseResult):
        parts = input
    else:
        parts = urlparse_cached(input)
    return f"{parts.scheme}://{parts.netloc}"


def get_etld1(url: str) -> str:
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}"


def get_focus_path(url: str) -> str | None:
    parts = urlparse_cached(url)
    if len(parts.path.split("/")) > 2:
        return "/".join(parts.path.split("/")[:-1]) + "/"
    return None


def _check_path(url_path: str, focus_element: str):
    if focus_element.startswith("/"):
        return url_path.startswith(focus_element)
    else:
        return focus_element in url_path


def is_allowed_path(input: str | Request | Response, focus_paths: set[str]) -> bool:
    if not focus_paths:
        return True
    url_path = urlparse_cached(input).path
    return any(_check_path(url_path.lower(), p.lower()) for p in focus_paths)


def validate_url(url: str) -> bool:
    result = urlparse(url)
    if result.scheme not in ("http", "https"):
        return False
    if not result.netloc:
        return False
    return True


def validate_domain(domain: str) -> bool:
    pattern = r"^([a-zA-Z0-9][a-zA-Z0-9\-]{1,61}[a-zA-Z0-9]\.)+[a-zA-Z]{2,}$"
    return bool(re.match(pattern, domain))
