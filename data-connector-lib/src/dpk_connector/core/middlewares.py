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
from typing import Any, Generator, Iterable

from scrapy import Spider, signals
from scrapy.crawler import Crawler
from scrapy.downloadermiddlewares.robotstxt import RobotsTxtMiddleware
from scrapy.downloadermiddlewares.stats import DownloaderStats
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response
from scrapy.http.request import NO_CALLBACK
from scrapy.robotstxt import ProtegoRobotParser, RobotParser
from scrapy.statscollectors import StatsCollector
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.python import to_unicode
from twisted.internet.defer import Deferred

from dpk_connector.core.item import ConnectorItem
from dpk_connector.core.utils import get_content_type, get_etld1, get_mime_type, get_netloc

logger = logging.getLogger(__name__)


class DelayingProtegoRobotParser(ProtegoRobotParser):
    """
    Robots.txt parser supporting crawl-delay/request-rate.
    """

    def __init__(self, robotstxt_body: str | bytes, spider: Spider):
        super().__init__(robotstxt_body, spider)
        self.max_delay = spider.crawler.settings.getfloat("ROBOTS_MAX_CRAWL_DELAY", 60)

    def delay(self, user_agent: str | bytes) -> float | None:
        user_agent = to_unicode(user_agent)
        crawl_delay = self.rp.crawl_delay(user_agent)
        request_rate = self.rp.request_rate(user_agent)
        if crawl_delay is None and request_rate is None:
            return None
        crawl_delay = crawl_delay or 0
        request_rate = (
            request_rate.seconds / request_rate.requests if request_rate else 0
        )
        delay = min(max(crawl_delay, request_rate), self.max_delay)
        return delay


class DelayingRobotsTxtMiddleware(RobotsTxtMiddleware):
    """
    Downloader middleware to follow crawl-delay/request-rate directives of robots.txt.
    """

    def __init__(self, crawler: Crawler, download_timeout: float):
        super().__init__(crawler)
        self.download_timeout = download_timeout
        self._delays: dict[str, float] = {}
        crawler.signals.connect(
            self._request_reached_downloader, signal=signals.request_reached_downloader
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        download_timeout = crawler.settings.getfloat("ROBOTSTXT_DOWNLOAD_TIMEOUT")
        if not download_timeout:
            download_timeout = crawler.settings.getfloat("DOWNLOAD_TIMEOUT")
        return cls(crawler, download_timeout)

    def _request_reached_downloader(self, request: Request, spider: Spider) -> None:
        key = request.meta.get("download_slot")
        if slot := self.crawler.engine.downloader.slots.get(key):
            parts = urlparse_cached(request)
            domain = parts.netloc
            if domain in self._delays:
                delay = self._delays[domain]
                if delay and slot.delay < delay:
                    slot.delay = delay
                    slot.randomize_delay = False

    def process_request_2(
        self, rp: RobotParser, request: Request, spider: Spider
    ) -> None:
        super().process_request_2(rp, request, spider)
        if isinstance(rp, DelayingProtegoRobotParser):
            parts = urlparse_cached(request)
            domain = parts.netloc
            if domain not in self._delays:
                user_agent = self._robotstxt_useragent
                if not user_agent:
                    user_agent = request.headers.get(
                        b"User-Agent", self._default_useragent
                    )
                delay = rp.delay(user_agent) or 0.0
                self._delays[domain] = delay
                if delay:
                    logger.info(
                        f"Set download delay to {delay} according to robots.txt. domain: {domain}"
                    )

    def robot_parser(self, request: Request, spider: Spider):
        url = urlparse_cached(request)
        netloc = url.netloc

        if netloc not in self._parsers:
            self._parsers[netloc] = Deferred()
            robotsurl = f"{url.scheme}://{url.netloc}/robots.txt"
            robotsreq = Request(
                robotsurl,
                priority=self.DOWNLOAD_PRIORITY,
                meta={
                    "dont_obey_robotstxt": True,
                    "system_request": True,
                    "download_timeout": self.download_timeout,
                },
                callback=NO_CALLBACK,
            )
            dfd = self.crawler.engine.download(robotsreq)
            dfd.addCallback(self._parse_robots, netloc, spider)
            dfd.addErrback(self._logerror, robotsreq, spider)
            dfd.addErrback(self._robots_error, netloc)
            self.crawler.stats.inc_value("robotstxt/request_count")

        if isinstance(self._parsers[netloc], Deferred):
            d = Deferred()

            def cb(result):
                d.callback(result)
                return result

            self._parsers[netloc].addCallback(cb)
            return d
        return self._parsers[netloc]


def _update_request_stats(
    stats: StatsCollector,
    request: Request,
    spider: Spider,
    prefix: str,
    skip_domains: bool = False,
):
    # request count
    stats.inc_value(prefix, spider=spider)
    # proxy distribution
    proxy = request.meta.get("proxy", "None")
    stats.inc_value(f"{prefix}/proxy/{proxy}", spider=spider)
    if not skip_domains:
        # domain distribution
        domain = get_etld1(to_unicode(request.url))
        stats.inc_value(f"{prefix}/domain/{domain}", spider=spider)
        # subdomain distribution
        sub_domain = get_netloc(request)
        stats.inc_value(f"{prefix}/subdomain/{sub_domain}", spider=spider)


def _update_stats(
    stats: StatsCollector,
    request: Request,
    response: Response,
    spider: Spider,
    prefix: str,
    skip_domains: bool = False,
):
    _update_request_stats(stats, request, spider, prefix, skip_domains)
    # mime type distribution
    content_type = get_content_type(response)
    if not content_type:
        stats.inc_value(f"{prefix}/mime_type/None", spider=spider)
    else:
        mime_type = get_mime_type(content_type)
        stats.inc_value(f"{prefix}/mime_type/{mime_type}", spider=spider)
    # status code distribution
    stats.inc_value(f"{prefix}/status_code/{response.status}", spider=spider)


def _update_sitemap_stats(stats: StatsCollector, spider: Spider, prefix: str):
    # sitemap
    stats.inc_value(f"{prefix}/sitemap", spider=spider)


class ConnectorRequestedStats(DownloaderStats):
    """
    Downloader middleware to expose additional stats.
    """

    def __init__(self, stats: StatsCollector, skip_domains: bool):
        super().__init__(stats)
        self.skip_domains = skip_domains

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        if not crawler.settings.getbool("DOWNLOADER_STATS"):
            raise NotConfigured
        skip_domains = crawler.settings.getbool("STATS_SKIP_DOMAINS")
        return cls(crawler.stats, skip_domains)

    def process_request(self, request: Request, spider: Spider):
        super().process_request(request, spider)
        prefix = "dpk_connector/requested"
        if not request.meta.get("system_request", False):
            _update_request_stats(
                self.stats, request, spider, prefix, self.skip_domains
            )
        if request.meta.get("sitemap", False):
            _update_sitemap_stats(self.stats, spider, prefix)

    def process_response(self, request: Request, response: Response, spider: Spider):
        ret = super().process_response(request, response, spider)
        prefix = "dpk_connector/accessed"
        if not request.meta.get("system_request", False):
            _update_stats(
                self.stats, request, response, spider, prefix, self.skip_domains
            )
        if request.meta.get("sitemap", False):
            _update_sitemap_stats(self.stats, spider, prefix)
        return ret


class ConnectorDownloadedStats:
    """
    Spider middleware to expose additional stats.
    """

    def __init__(self, stats: StatsCollector, skip_domains: bool):
        self.stats = stats
        self.skip_domains = skip_domains

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        if not crawler.settings.getbool("DOWNLOADER_STATS"):
            raise NotConfigured
        skip_domains = crawler.settings.getbool("STATS_SKIP_DOMAINS")
        return cls(crawler.stats, skip_domains)

    def process_spider_output(
        self,
        response: Response,
        result: Iterable[Request | ConnectorItem],
        spider: Spider,
    ) -> Generator[Any, Any, None]:
        for r in result:
            if isinstance(r, ConnectorItem):
                if (not r.system_request) and r.downloaded:
                    _update_stats(
                        self.stats,
                        response.request,
                        response,
                        spider,
                        "dpk_connector/downloaded",
                        self.skip_domains,
                    )
                if r.sitemap:
                    _update_sitemap_stats(self.stats, spider, "dpk_connector/downloaded")
            yield r
