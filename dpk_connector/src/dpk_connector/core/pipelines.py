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

from typing import Any
from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.exceptions import DropItem

from dpk_connector.core.item import ConnectorItem


class DropPipeline:
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls()

    def process_item(self, item: ConnectorItem, spider: Spider) -> Any:
        if item.system_request or (not item.downloaded):
            raise DropItem
        return item
