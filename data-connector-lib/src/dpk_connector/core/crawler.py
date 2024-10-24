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

import threading
from typing import Any, Callable, Collection, Type, cast

from scrapy import Spider
from scrapy.crawler import Crawler, CrawlerRunner
from scrapy.settings import Settings
from twisted.internet.defer import Deferred

from dpk_connector.core.utils import validate_domain, validate_url

_lock = threading.Lock()
_reactor_initialized = False
_reactor_started = False


def _run_reactor():
    from twisted.internet import reactor

    reactor.run(installSignalHandlers=False)


_reactor_thread: threading.Thread = threading.Thread(
    target=_run_reactor,
    daemon=True,
)


def _start_reactor():
    with _lock:
        global _reactor_started
        if not _reactor_started:
            _reactor_thread.start()
            _reactor_started = True


def _stop_reactor():
    from twisted.internet import reactor

    try:
        reactor.stop()
    except RuntimeError:
        pass


class MultiThreadedCrawlerRunner(CrawlerRunner):
    def _create_crawler(self, spidercls: str | type[Spider]) -> Crawler:
        if isinstance(spidercls, str):
            spidercls = self.spider_loader.load(spidercls)
        with _lock:
            global _reactor_initialized
            init_reactor = not _reactor_initialized
            crawler = Crawler(
                cast(Type[Spider], spidercls), self.settings, init_reactor
            )
            _reactor_initialized = True
        return crawler


def async_crawl(
    seed_urls: Collection[str],
    on_downloaded: Callable[[str, bytes, dict[str, str]], None],
    user_agent: str = "",
    headers: dict[str, str] = {},
    allow_domains: Collection[str] = (),
    subdomain_focus: bool = False,
    path_focus: bool = False,
    allow_mime_types: Collection[str] = (
        "application/pdf",
        "text/html",
        "text/markdown",
        "text/plain",
    ),
    disallow_mime_types: Collection[str] = (),
    depth_limit: int = -1,
    download_limit: int = -1,
    concurrent_requests: int = 20,
    concurrent_requests_per_domain: int = 10,
    download_delay: float = 0,
    randomize_download_delay: bool = True,
    download_timeout: float = 180,
    autothrottle_enabled: bool = True,
    autothrottle_max_delay: float = 300,
    autothrottle_target_concurrency: float = 10,
    robots_max_crawl_delay: float = 60,
) -> Deferred[None]:
    # Assisted by WCA@IBM
    # Latest GenAI contribution: ibm/granite-20b-code-instruct-v2
    """
    Do crawl asynchronously.

    Parameters:
        seed_urls (Collection[str]): A collection of seed URLs to start the crawl from.
        on_downloaded (Callable[[str, bytes, dict[str, str]], None]): The callback function to be called for each downloaded page.
        user_agent (str): The user agent string to use for the crawler. Defaults to "Scrapy/VERSION (+https://scrapy.org)".
        headers (dict[str, str]): A dictionary of additional headers to send with each request. Default is an empty dictionary.
        allow_domains (Collection[str]): A collection of domains to restrict the crawler to. Default is the domains of the seed URLs.
        subdomain_focus (bool): If specified, only links under the subdomains of the input seed URLs will be extracted. Ignored if `allow_domains` is specified.
        path_focus (bool): If specified, only links under the paths of the input seed URLs will be extracted.
        allow_mime_types (Collection[str]): A collection of MIME types to allow during the crawl. Default is a collection containing "application/pdf", "text/html", "text/markdown", and "text/plain".
        disallow_mime_types (Collection[str]): A collection of MIME types to disallow during the crawl. Default is an empty collection.
        depth_limit (int): The maximum depth of the crawl. Default is -1, which means no limit.
        download_limit (int): The maximum number of pages to download. Default is -1, which means no limit. This is a soft limit, meaning that a crawler may download more pages than the specified limit.
        concurrent_requests (int): The maximum number of concurrent requests to make. Default is 20.
        concurrent_requests_per_domain (int): The maximum number of concurrent requests to make per domain. Default is 10.
        download_delay (float): The delay between consecutive requests. Default is 0.
        randomize_download_delay (bool): If specified, the download delay will be randomized between 0.5 * `download_delay and 1.5 * `download_delay`. Default is True.
        download_timeout (float): The timeout for each request. Default is 180 seconds.
        autothrottle_enabled (bool): If specified, autothrottling will be enabled. Default is True.
        autothrottle_max_delay (float): The maximum delay between consecutive requests when autothrottling is enabled. Default is 300 seconds.
        autothrottle_target_concurrency (float): The target concurrency for autothrottling. Default is 10.
        robots_max_crawl_delay (float): The maximum crawl delay allowed by the robots.txt file. Default is 60 seconds.

    Returns:
        Deferred[None]: A Twisted deferred object that can be used to wait for the crawler to finish.
    """
    if not seed_urls:
        raise ValueError("Empty seed URLs.")
    for url in seed_urls:
        if not validate_url(url):
            raise ValueError(f"Seed URL {url} is not valid.")
    for domain in allow_domains:
        if not validate_domain(domain):
            raise ValueError(f"Allow domain {domain} is not valid.")
    if depth_limit < -1:
        raise ValueError(f"Invalid depth limit {depth_limit}")
    if download_limit < -1:
        raise ValueError(f"Invalid download limit {download_limit}")
    if concurrent_requests < 1:
        raise ValueError(f"Invalid concurrent requests {concurrent_requests}")
    if concurrent_requests_per_domain < 1:
        raise ValueError(
            f"Invalid concurrent reuqests per domain {concurrent_requests_per_domain}"
        )
    if download_delay < 0:
        raise ValueError(f"Invalid download delay {download_delay}")
    if download_timeout < 0:
        raise ValueError(f"Invalid donwload timeout {download_timeout}")
    if autothrottle_max_delay < 0:
        raise ValueError(f"Invalid autothrottle max delay {autothrottle_max_delay}")
    if autothrottle_target_concurrency < 1:
        raise ValueError(
            f"Invalid autothrottle target concurrency {autothrottle_target_concurrency}"
        )
    if robots_max_crawl_delay < 0:
        raise ValueError(f"Invalid robots max crawl delay {robots_max_crawl_delay}")

    settings = Settings()
    settings.setmodule("dpk_connector.core.settings", priority="project")

    if user_agent:
        settings.set("USER_AGENT", user_agent, priority="spider")
    if headers:
        settings.set("DEFAULT_REQUEST_HEADERS", headers, priority="spider")
    if depth_limit == 0:
        depth_limit = -1
    elif depth_limit == -1:
        depth_limit = 0
    settings.set("DEPTH_LIMIT", depth_limit, priority="spider")
    if download_limit == -1:
        download_limit = 0
    settings.set("CLOSESPIDER_ITEMCOUNT", download_limit, priority="spider")
    settings.set("CONCURRENT_REQUESTS", concurrent_requests, priority="spider")
    settings.set(
        "CONCURRENT_REQUESTS_PER_DOMAIN",
        concurrent_requests_per_domain,
        priority="spider",
    )
    settings.set("DOWNLOAD_DELAY", download_delay, priority="spider")
    settings.set(
        "RANDOMIZE_DOWNLOAD_DELAY", randomize_download_delay, priority="spider"
    )
    settings.set("DOWNLOAD_TIMEOUT", download_timeout, priority="spider")
    settings.set("AUTOTHROTTLE_ENABLED", autothrottle_enabled, priority="spider")
    settings.set("AUTOTHROTTLE_MAX_DELAY", autothrottle_max_delay, priority="spider")
    settings.set(
        "AUTOTHROTTLE_TARGET_CONCURRENCY",
        autothrottle_target_concurrency,
        priority="spider",
    )
    settings.set("ROBOTS_MAX_CRAWL_DELAY", robots_max_crawl_delay, priority="spider")

    runner = MultiThreadedCrawlerRunner(settings)
    runner.crawl(
        "dpk-connector-sitemap",
        seed_urls=seed_urls,
        callback=on_downloaded,
        allow_domains=allow_domains,
        subdomain_focus=subdomain_focus,
        path_focus=path_focus,
        allow_mime_types=allow_mime_types,
        disallow_mime_types=disallow_mime_types,
        disable_sitemap_search=True,
    )
    _start_reactor()
    return runner.join()


def crawl(
    seed_urls: Collection[str],
    on_downloaded: Callable[[str, bytes, dict[str, str]], None],
    user_agent: str = "",
    headers: dict[str, str] = {},
    allow_domains: Collection[str] = (),
    subdomain_focus: bool = False,
    path_focus: bool = False,
    allow_mime_types: Collection[str] = (
        "application/pdf",
        "text/html",
        "text/markdown",
        "text/plain",
    ),
    disallow_mime_types: Collection[str] = (),
    depth_limit: int = -1,
    download_limit: int = -1,
    concurrent_requests: int = 20,
    concurrent_requests_per_domain: int = 10,
    download_delay: float = 0,
    randomize_download_delay: bool = True,
    download_timeout: float = 180,
    autothrottle_enabled: bool = True,
    autothrottle_max_delay: float = 300,
    autothrottle_target_concurrency: float = 10,
    robots_max_crawl_delay: float = 60,
) -> None:
    # Assisted by WCA@IBM
    # Latest GenAI contribution: ibm/granite-20b-code-instruct-v2
    """
    Do crawl synchronously.

    Parameters:
        seed_urls (Collection[str]): A collection of seed URLs to start the crawl from.
        on_downloaded (Callable[[str, bytes, dict[str, str]], None]): The callback function to be called for each downloaded page.
        user_agent (str): The user agent string to use for the crawler. Defaults to "Scrapy/VERSION (+https://scrapy.org)".
        headers (dict[str, str]): A dictionary of additional headers to send with each request. Default is an empty dictionary.
        allow_domains (Collection[str]): A collection of domains to restrict the crawler to. Default is the domains of the seed URLs.
        subdomain_focus (bool): If specified, only links under the subdomains of the input seed URLs will be extracted. Ignored if `allow_domains` is specified.
        path_focus (bool): If specified, only links under the paths of the input seed URLs will be extracted.
        allow_mime_types (Collection[str]): A collection of MIME types to allow during the crawl. Default is a collection containing "application/pdf", "text/html", "text/markdown", and "text/plain".
        disallow_mime_types (Collection[str]): A collection of MIME types to disallow during the crawl. Default is an empty collection.
        depth_limit (int): The maximum depth of the crawl. Default is -1, which means no limit.
        download_limit (int): The maximum number of pages to download. Default is -1, which means no limit. This is a soft limit, meaning that a crawler may download more pages than the specified limit.
        concurrent_requests (int): The maximum number of concurrent requests to make. Default is 20.
        concurrent_requests_per_domain (int): The maximum number of concurrent requests to make per domain. Default is 10.
        download_delay (float): The delay between consecutive requests. Default is 0.
        randomize_download_delay (bool): If specified, the download delay will be randomized between 0.5 * `download_delay and 1.5 * `download_delay`. Default is True.
        download_timeout (float): The timeout for each request. Default is 180 seconds.
        autothrottle_enabled (bool): If specified, autothrottling will be enabled. Default is True.
        autothrottle_max_delay (float): The maximum delay between consecutive requests when autothrottling is enabled. Default is 300 seconds.
        autothrottle_target_concurrency (float): The target concurrency for autothrottling. Default is 10.
        robots_max_crawl_delay (float): The maximum crawl delay allowed by the robots.txt file. Default is 60 seconds.

    Returns:
        None
    """
    condition = threading.Condition()

    def on_completed(result: Any):
        with condition:
            condition.notify()

    d = async_crawl(
        seed_urls,
        on_downloaded,
        user_agent,
        headers,
        allow_domains,
        subdomain_focus,
        path_focus,
        allow_mime_types,
        disallow_mime_types,
        depth_limit,
        download_limit,
        concurrent_requests,
        concurrent_requests_per_domain,
        download_delay,
        randomize_download_delay,
        download_timeout,
        autothrottle_enabled,
        autothrottle_max_delay,
        autothrottle_target_concurrency,
        robots_max_crawl_delay,
    )
    d.addBoth(on_completed)
    with condition:
        condition.wait()


def shutdown():
    """
    Shutdown all crawls.
    """
    _stop_reactor()
