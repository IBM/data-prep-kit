# DPK Connector Overview

The Data Prep Kit Connector (DPK Connector) is a Python library for scalable and compliant web crawling.

Features:
- Robots.txt compliant: The Connector follows allow/disallow lists and some extended directives such as `Crawl-delay` in robots.txt of websites.
- Sitemap support: The Connector automatically parses sitemap urls from input and tries to find them from robots.txt.
- User agent and headers customization: You can use your own user agent string and request headers.
- Domain and path focus: You can limit domains and paths accessed by the library.
- Mime type filters: You can restrict mime types which can be downloaded.
- Parallel processing: Requests to websites are processed in parallel.

## Example usage

```python
from dpk_connector import crawl, shutdown


def main():
    """
    An example of running a crawl.
    """

    def on_downloaded(url: str, body: bytes, headers: dict) -> None:
        """
        Callback function called when a page has been downloaded.
        You have access to the request URL, response body and headers.
        """
        print(f"url: {url}, headers: {headers}, body: {body[:64]}")

    user_agent = "Mozilla/5.0 (X11; Linux i686; rv:125.0) Gecko/20100101 Firefox/125.0"

    # Start crawling
    crawl(
        ["https://crawler-test.com/"],
        on_downloaded,
        user_agent=user_agent,
        depth_limit=0,
    )  # blocking call

    # Shutdown all crawls
    shutdown()


if __name__ == "__main__":
    main()
```
