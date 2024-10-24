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

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, concurrent_requests=-10)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, concurrent_requests_per_domain=-10)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, download_delay=-0.1)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, download_timeout=-0.1)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, autothrottle_max_delay=-0.1)
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(
            ["http://example.com"], on_downloaded, autothrottle_target_concurrency=0.5
        )
    assert isinstance(e.value, ValueError) is True

    with pytest.raises(ValueError) as e:
        crawl(["http://example.com"], on_downloaded, robots_max_crawl_delay=-0.1)
    assert isinstance(e.value, ValueError) is True
