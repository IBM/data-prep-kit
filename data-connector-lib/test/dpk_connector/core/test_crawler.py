import pytest

from dpk_connector.core.crawler import crawl


def test_invalid_crawler():
    def on_downloaded(url: str, body: bytes, headers: dict[str, str]):
        pass

    with pytest.raises(ValueError) as e:
        crawl([], on_downloaded)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["invalidseedurl"], on_downloaded)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, allow_domains=("invaliddomain",))
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, depth_limit=-10)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, download_limit=-10)
    assert isinstance(e.value, ValueError) is True
