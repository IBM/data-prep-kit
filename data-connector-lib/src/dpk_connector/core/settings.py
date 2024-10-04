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

BOT_NAME = "dpk-connector"

SPIDER_MODULES = ["dpk_connector.core.spiders"]

# Robots
ROBOTSTXT_OBEY = True
ROBOTS_MAX_CRAWL_DELAY = 60
ROBOTSTXT_PARSER = "dpk_connector.core.middlewares.DelayingProtegoRobotParser"

# Downloader parameters
CONCURRENT_REQUESTS = 20
CONCURRENT_REQUESTS_PER_DOMAIN = 10
DOWNLOAD_DELAY = 0
RANDOMIZE_DOWNLOAD_DELAY = True
DOWNLOAD_TIMEOUT = 180

# Autothrottle
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0
AUTOTHROTTLE_MAX_DELAY = 300
AUTOTHROTTLE_TARGET_CONCURRENCY = 10
AUTOTHROTTLE_DEBUG = False

# Middlewares/pipelines/extensions
SPIDER_MIDDLEWARES = {
    "dpk_connector.core.middlewares.ConnectorDownloadedStats": 10,
}
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware": None,
    "dpk_connector.core.middlewares.DelayingRobotsTxtMiddleware": 100,
    "scrapy.downloadermiddlewares.stats.DownloaderStats": None,
    "dpk_connector.core.middlewares.ConnectorRequestedStats": 850,
}
ITEM_PIPELINES = {
    "dpk_connector.core.pipelines.DropPipeline": 100,
}
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
    "scrapy.extensions.memdebug.MemoryDebugger": None,
}

# Queue
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.LifoMemoryQueue"

# Logging
LOG_LEVEL = "INFO"
LOG_SCRAPED_ITEMS = False
LOG_FORMATTER = "dpk_connector.core.logging.QuietLogFormatter"

# Periodic logging
PERIODIC_LOG_DELTA = True
PERIODIC_LOG_STATS = True
PERIODIC_LOG_TIMING_ENABLED = True
LOGSTATS_INTERVAL = 300

# Misc
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
