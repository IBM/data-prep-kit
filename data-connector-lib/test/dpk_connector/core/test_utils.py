# Assisted by WCA@IBM
# Latest GenAI contribution: ibm/granite-20b-code-instruct-v2

import pytest
from dpk_connector.core.utils import (
    get_base_url,
    get_content_type,
    get_etld1,
    get_focus_path,
    get_header_value,
    get_mime_type,
    is_allowed_path,
    urlparse_cached,
    validate_domain,
    validate_url,
)
from pytest_mock import MockerFixture
from scrapy.http import Request, Response


def test_get_header_value():
    response = Response(
        "http://example.com", headers={"Content-Type": "application/json"}
    )
    assert get_header_value(response, "Content-Type") == "application/json"


def test_get_header_value_not_found():
    response = Response("http://example.com")
    assert get_header_value(response, "Content-Type") is None


def test_get_content_type():
    response = Response("http://example.com", headers={"Content-Type": "text/html"})
    assert get_content_type(response) == "text/html"


def test_get_content_type_not_found():
    response = Response("http://example.com")
    assert get_content_type(response) is None


def test_urlparse_cached_with_str(mocker: MockerFixture):
    import dpk_connector.core.utils

    spy = mocker.spy(dpk_connector.core.utils, "urlparse")
    urlparse_cached("http://example.com")
    spy.assert_called_once_with("http://example.com")


def test_urlparse_cached_with_request(mocker: MockerFixture):
    import dpk_connector.core.utils

    spy = mocker.spy(dpk_connector.core.utils, "_urlparse_cached")
    request = Request("http://example.com")
    urlparse_cached(request)
    spy.assert_called_once_with(request)


def test_urlparse_cached_with_response(mocker: MockerFixture):
    import dpk_connector.core.utils

    spy = mocker.spy(dpk_connector.core.utils, "_urlparse_cached")
    response = Response("http://example.com")
    urlparse_cached(response)
    spy.assert_called_once_with(response)


def test_get_base_url():
    url = "http://localhost:8000/test?param=value"
    assert get_base_url(url) == "http://localhost:8000"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://www.example.com", "example.com"),
        ("https://www.example.co.uk", "example.co.uk"),
        ("http://www.example.com/path?query=string#fragment", "example.com"),
    ],
)
def test_get_etld1(url: str, expected: str):
    assert get_etld1(url) == expected


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://www.example.com", None),
        ("https://www.example.com/page", None),
        ("https://www.example.com/page/", "/page/"),
        ("https://www.example.com/page/subpage", "/page/"),
        ("https://www.example.com/page/subpage/", "/page/subpage/"),
    ],
)
def test_get_focus_path(url: str, expected: str | None):
    assert get_focus_path(url) == expected


@pytest.mark.parametrize(
    "url,paths,expected",
    [
        ("http://localhost/", set(), True),
        ("http://localhost/test/", {"/test/"}, True),
        ("http://localhost/test/", {"/test2/"}, False),
        ("http://localhost/test", {"/test/"}, False),
        ("http://localhost/test/", {"/test/", "/test2/"}, True),
        ("http://localhost/test3/", {"/test/", "/test2/"}, False),
    ],
)
def test_is_allowed_path(url: str, paths: set[str], expected: bool):
    assert is_allowed_path(url, paths) is expected


@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://www.cwi.nl:80/%7Eguido/Python.html", True),
        ("/data/Python.html", False),
        ("532", False),
        ("dkakasdkjdjakdjadjfalskdjfalk", False),
        ("https://stackoverflow.com", True),
    ],
)
def test_validate_url(url: str, expected: bool):
    assert validate_url(url) is expected


@pytest.mark.parametrize(
    "content_type,expected",
    [
        ("text/html", "text/html"),
        ("application/json; charset=utf-8", "application/json"),
    ],
)
def test_get_mime_type(content_type: str, expected: str):
    assert get_mime_type(content_type) == expected


@pytest.mark.parametrize(
    "domain,expected",
    [
        ("example.com", True),
        ("sub-domain.example.com", True),
        ("a" * 254 + ".com", False),
        ("", False),
        ("example", False),
        ("sub_domain.example.com", False),
    ],
)
def test_validate_domain(domain: str, expected: bool):
    assert validate_domain(domain) is expected
