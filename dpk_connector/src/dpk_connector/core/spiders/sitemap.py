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
from typing import Any, Callable, Collection, Generator
from urllib.parse import ParseResult

from scrapy import Request
from scrapy.http import Response
from scrapy.link import Link
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import SitemapSpider
from scrapy.spiders.sitemap import iterloc
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots

from dpk_connector.core.item import ConnectorItem
from dpk_connector.core.utils import (
    get_base_url,
    get_content_type,
    get_etld1,
    get_focus_path,
    is_allowed_path,
    urlparse_cached,
)


class BaseSitemapSpider(SitemapSpider):
    SITEMAP_DOWNLOAD_PRIORITY = 10

    name = "base-sitemap"

    def __init__(
        self,
        seed_urls: Collection[str],
        allow_domains: Collection[str] = (),
        path_focus: bool = False,
        allow_mime_types: Collection[str] = (),
        disallow_mime_types: Collection[str] = (),
        depth_limit: int = 0,
        disable_sitemap_search: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.depth_limit = depth_limit
        self.sitemap_search = (not disable_sitemap_search) and depth_limit >= 0

        # Build sitemap url candidates
        self.input_seed_urls = seed_urls
        sitemap_urls = []
        sitemaps_seen = []
        for seed_url in seed_urls:
            parts = urlparse_cached(seed_url)
            if seed_url.endswith(
                (
                    ".xml",
                    ".xml.gz",
                    "/robots.txt",
                    "robots.txt/",
                    "/sitemap",
                    "/sitemap/",
                )
            ):
                sitemap_urls.append(seed_url)
            elif self.sitemap_search:
                sitemap_urls.extend(self._get_sitemap_urls(parts))
            sitemaps_seen.append(parts.netloc)
        self.seed_urls = set(seed_urls) - set(sitemap_urls)
        self.sitemap_urls = set(sitemap_urls)
        self.sitemaps_seen = set(sitemaps_seen)

        # Extract focus paths
        self.focus_paths: set[str] = set()
        if path_focus:
            for seed_url in self.seed_urls:
                path = get_focus_path(seed_url)
                if path is not None:
                    self.focus_paths.add(path)

        # Domains and mime types filtering
        self.allowed_domains = set(
            allow_domains
            if len(allow_domains) > 0
            else [get_etld1(url) for url in seed_urls]
        )
        self.allow_mime_types = set(
            [m.lower() for m in allow_mime_types] if len(allow_mime_types) > 0 else ()
        )
        self.disallow_mime_types = set(
            [m.lower() for m in disallow_mime_types]
            if len(disallow_mime_types) > 0
            else ()
        )

        # Link extraction from html
        self.link_extractor = LinkExtractor(
            allow_domains=self.allowed_domains,
            unique=True,
            deny_extensions=(),
            tags=("a", "area", "link"),
        )

        self.log(
            f"Seed URLs: {self.seed_urls}, sitemap URLs: {self.sitemap_urls}, allow domains: {self.allowed_domains}, focus paths: {self.focus_paths}, allow mime types: {self.allow_mime_types}, disallow mime types: {self.disallow_mime_types}, depth limit: {self.depth_limit}, sitemap search: {self.sitemap_search}",
            logging.INFO,
        )

    def _get_sitemap_urls(self, parts: ParseResult) -> list[str]:
        base_url = get_base_url(parts)
        sitemap_variations = (
            "robots.txt",
            "robots.txt/",
            "sitemap.xml",
            "sitemap_index.xml",
            "sitemapindex.xml",
            "sitemap",
            "sitemap-index.xml",
            "sitemap/index.xml",
            "sitemap/sitemap.xml",
            "sitemap1.xml",
        )
        return [f"{base_url}/{sitemap}" for sitemap in sitemap_variations]

    def start_requests(self):
        for url in self.sitemap_urls:
            yield Request(
                url,
                self._parse_sitemap,
                priority=self.SITEMAP_DOWNLOAD_PRIORITY,
                meta={
                    "seed_url": url,
                    "previous_url": "",
                    "system_request": True,
                    "sitemap": True,
                },
            )
        for url in self.seed_urls:
            yield Request(
                url,
                self.parse,
                meta={
                    "seed_url": url,
                    "previous_url": "",
                },
            )

    def _parse_sitemap(self, response: Response):
        yield ConnectorItem(dropped=False, downloaded=False, system_request=True, sitemap=True)

        seed_url = response.meta["seed_url"]

        if response.url.endswith("/robots.txt") or response.url.endswith(
            "/robots.txt/"
        ):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(
                    url,
                    callback=self._parse_sitemap,
                    priority=self.SITEMAP_DOWNLOAD_PRIORITY,
                    meta={
                        "seed_url": seed_url,
                        "previous_url": response.url,
                        "system_request": True,
                        "sitemap": True,
                    },
                )
        else:
            body = self._get_sitemap_body(response)
            if not body:
                self.log(
                    f"Ignoring invalid sitemap: {response}",
                    logging.WARN,
                    extra={"spider": self},
                )
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            if s.type == "sitemapindex":
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(
                        x.search(loc) for x in self._follow
                    ) and self._is_allowed_path(loc):
                        yield Request(
                            loc,
                            callback=self._parse_sitemap,
                            priority=self.SITEMAP_DOWNLOAD_PRIORITY,
                            meta={
                                "seed_url": seed_url,
                                "previous_url": response.url,
                                "system_request": True,
                                "sitemap": True,
                            },
                        )
            elif s.type == "urlset":
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc) and self._is_allowed_path(loc):
                            yield Request(
                                loc,
                                callback=c,
                                meta={
                                    "seed_url": seed_url,
                                    "previous_url": response.url,
                                },
                            )
                            break

    def _is_allowed_path(self, input: str | Request | Response) -> bool:
        return is_allowed_path(input, self.focus_paths)

    def _is_allowed_content_type(self, content_type: str) -> bool:
        return any([mtype in content_type for mtype in self.allow_mime_types])

    def _is_disallowed_content_type(self, content_type: str) -> bool:
        return any([mtype in content_type for mtype in self.disallow_mime_types])

    def _should_download(self, content_type: str | None) -> bool:
        if (not self.allow_mime_types) and (not self.disallow_mime_types):
            return True
        if not content_type:
            return False
        ctype = content_type.lower()
        if not self.allow_mime_types:
            return not self._is_disallowed_content_type(ctype)
        if not self.disallow_mime_types:
            return self._is_allowed_content_type(ctype)
        return (
            not self._is_disallowed_content_type(ctype)
        ) and self._is_allowed_content_type(ctype)

    def _explore_sitemap(self, response: Response) -> Generator[Request, Any, None]:
        depth = response.meta.get("depth", 0)
        depth_limit = self.depth_limit
        if (depth_limit == 0 or depth < depth_limit) and self.sitemap_search:
            parts = urlparse_cached(response)
            domain = parts.netloc
            if domain not in self.sitemaps_seen:
                self.log(
                    f"New domain {domain} found. Search for sitemap.", logging.INFO
                )
                self.sitemaps_seen.add(domain)
                for sitemap in self._get_sitemap_urls(parts):
                    yield Request(
                        sitemap,
                        callback=self._parse_sitemap,
                        priority=self.SITEMAP_DOWNLOAD_PRIORITY,
                        meta={
                            "seed_url": response.meta["seed_url"],
                            "previous_url": response.url,
                            "system_request": True,
                            "sitemap": True,
                        },
                    )

    def _explore_links(
        self, response: Response, links: list[Link]
    ) -> Generator[Request, Any, None]:
        depth = response.meta.get("depth", 0)
        depth_limit = self.depth_limit
        if depth_limit == 0 or depth < depth_limit:
            for link in links:
                if self._is_allowed_path(link.url):
                    yield Request(
                        link.url,
                        callback=self.parse,
                        meta={
                            "seed_url": response.meta["seed_url"],
                            "previous_url": response.url,
                        },
                    )


class ConnectorSitemapSpider(BaseSitemapSpider):
    name = "dpk-connector-sitemap"

    def __init__(
        self,
        callback: Callable[[str, bytes, dict[str, str]], None],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.callback = callback

    def parse(
        self, response: Response, **kwargs: Any
    ) -> Generator[Request | ConnectorItem, Any, None]:
        drop = False
        content_type = get_content_type(response)
        if not content_type:
            drop = True
        is_html = "text/html" in content_type.lower()
        should_download = self._should_download(content_type)
        if not (is_html or should_download):
            drop = True
        if drop:
            yield ConnectorItem(
                dropped=True, downloaded=False, system_request=False, sitemap=False
            )
            return

        # Download contents
        if should_download:
            self.callback(
                str(response.url), response.body, response.headers.to_unicode_dict()
            )
            # to count up downloaded pages and collect stats
            yield ConnectorItem(
                dropped=False, downloaded=True, system_request=False, sitemap=False
            )
        else:
            yield ConnectorItem(
                dropped=False, downloaded=False, system_request=False, sitemap=False
            )

        # Search for sitemap
        yield from self._explore_sitemap(response)

        # Extract links and dispatch them
        links = self.link_extractor.extract_links(response) if is_html else []
        yield from self._explore_links(response, links)
