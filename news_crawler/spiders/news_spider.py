import scrapy
import csv
import os
from urllib.parse import urlparse
import pandas as pd
from collections import Counter
import twisted.internet.error

class NewsSpider(scrapy.Spider):
    name = "news_spider"

    NEWS_SITE_NAME = "LA-Times"
    START_URL = "https://www.latimes.com/"
    DEPTH_LIMIT = 16

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 1,
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408],
        "REDIRECT_MAX_TIMES": 5,
        "DEPTH_LIMIT": 16,
        "CLOSESPIDER_PAGECOUNT": 20000
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetched_count = 0
        self.success_count = 0
        self.failed_count = 0
        self.aborted_count = 0
        self.crawler = kwargs.get('crawler')

        self.fetch_file = f"fetch_{self.NEWS_SITE_NAME}.csv"
        self.visit_file = f"visit_{self.NEWS_SITE_NAME}.csv"
        self.urls_file = f"urls_{self.NEWS_SITE_NAME}.csv"
        self.report_file = f"CrawlReport_{self.NEWS_SITE_NAME}.txt"

        for file in [self.fetch_file, self.visit_file, self.urls_file, self.report_file]:
            if os.path.exists(file):
                os.remove(file)

        self.fetch_csv = open(self.fetch_file, "w", newline="", encoding="utf-8")
        self.visit_csv = open(self.visit_file, "w", newline="", encoding="utf-8")
        self.urls_csv = open(self.urls_file, "w", newline="", encoding="utf-8")
        self.report_txt = open(self.report_file, "w", encoding="utf-8")

        self.fetch_writer = csv.writer(self.fetch_csv)
        self.visit_writer = csv.writer(self.visit_csv)
        self.urls_writer = csv.writer(self.urls_csv)

        self.fetch_writer.writerow(["URL", "Status_Code"])
        self.visit_csv.flush()
        self.visit_writer.writerow(["URL", "Size (Bytes)", "Outlinks", "Content-Type"])
        self.visit_csv.flush()
        self.urls_writer.writerow(["Encountered URL", "Indicator"])
        self.urls_csv.flush()

    def start_requests(self):
        yield scrapy.Request(self.START_URL, callback=self.parse, errback=self.handle_error, meta={'depth': 0})

    def parse(self, response):
        self.logger.info(f"Parsing: {response.url} - {response.status}")

        url = response.url.replace(",", "-")
        status_code = response.status
        protocol = urlparse(url).scheme.upper()
        self.fetched_count += 1

        redirected_url = response.request.meta.get('redirect_urls', [""])[0] if 'redirect_urls' in response.request.meta else ""
        self.log_fetch(url, status_code, redirected_url)

        if urlparse(url).netloc != urlparse(self.START_URL).netloc:
            return

        if 200 <= status_code < 300:
            self.success_count += 1
            content_type = response.headers.get("Content-Type", b"").decode().split(";")[0]
            
            allowed_types = [
                "text/html",
                "application/pdf",
                "image/jpeg",
                "image/png",
                "image/gif",
                "application/msword",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ]
            if any(ext in content_type for ext in allowed_types):
                page_size = len(response.body)
                outlinks = set()

                for link in response.css("a::attr(href)").getall():
                    absolute_url = response.urljoin(link)
                    if not absolute_url.startswith(("http://", "https://")):
                        continue
                    
                    indicator = "OK" if urlparse(absolute_url).netloc == urlparse(self.START_URL).netloc else "N_OK"
                    self.log_urls(absolute_url, indicator)
                    outlinks.add(absolute_url)
                    if urlparse(absolute_url).netloc == urlparse(self.START_URL).netloc:
                        yield scrapy.Request(absolute_url, callback=self.parse, errback=self.handle_error, meta={'depth': response.meta['depth'] + 1})

                self.log_visit(url, page_size, len(outlinks), content_type)
            else:
                self.logger.info(f"Skipping URL due to unsupported content type: {url} - {content_type}")
       

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}, reason: {failure.getErrorMessage()}")

        status_code = 0 

        
        if failure.check(scrapy.spidermiddlewares.httperror.HttpError):
            status_code = failure.value.response.status  

       
        elif failure.check(
            scrapy.core.downloader.handlers.http11.TunnelError,
            scrapy.downloadermiddlewares.retry.RetryMiddleware,
            scrapy.downloadermiddlewares.redirect.RedirectMiddleware,
            twisted.internet.error.TimeoutError,
            twisted.internet.error.ConnectionRefusedError,
            twisted.internet.error.ConnectionLost,
            twisted.internet.error.DNSLookupError
        ):
            status_code = 408  

        
        else:
            status_code = 500 

        self.log_fetch(failure.request.url.replace(",", "-"), status_code)

    def log_fetch(self, url, status, redirected_url=""):
        self.logger.debug(f"Writing fetch: {url}, {status}")
        self.fetch_writer.writerow([url, status])
        if redirected_url:
            self.urls_writer.writerow([redirected_url, "OK" if urlparse(redirected_url).netloc == urlparse(self.START_URL).netloc else "N_OK"])
        self.fetch_csv.flush()

    def log_visit(self, url, size, outlinks, content_type):
        self.visit_writer.writerow([url, size, outlinks, content_type])
        self.visit_csv.flush()

    def log_urls(self, url, indicator):
        self.urls_writer.writerow([url, indicator])


    def closed(self, reason):
        fetch_df = pd.DataFrame(columns=["URL", "Status"]) if os.stat(self.fetch_file).st_size == 0 else pd.read_csv(self.fetch_file)
        visit_df = pd.DataFrame(columns=["URL", "Size (Bytes)", "Outlinks", "Content-Type"]) if os.stat(self.visit_file).st_size == 0 else pd.read_csv(self.visit_file)
        urls_df = pd.DataFrame(columns=["Encountered URL", "Indicator"]) if os.stat(self.urls_file).st_size == 0 else pd.read_csv(self.urls_file)

        self.report_txt.write(f"Name: Sricharan Koride\n")
        self.report_txt.write(f"USC ID: 2343517466\n")  
        self.report_txt.write(f"News site crawled: {self.START_URL}\n")
        self.report_txt.write("Number of threads: 8\n\n")

        self.report_txt.write("Fetch Statistics\n")
        self.report_txt.write(f"# fetches attempted: {self.fetched_count}\n")
        self.report_txt.write(f"# fetches succeeded: {self.success_count}\n")
        self.report_txt.write(f"# fetches failed or aborted: {self.failed_count + self.aborted_count}\n\n")

        total_urls = visit_df["Outlinks"].sum() if not visit_df.empty else 0
        unique_urls = urls_df["Encountered URL"].nunique() if not urls_df.empty else 0
        unique_within = urls_df[urls_df["Indicator"] == "OK"]["Encountered URL"].nunique() if not urls_df.empty else 0
        unique_outside = urls_df[urls_df["Indicator"] == "N_OK"]["Encountered URL"].nunique() if not urls_df.empty else 0
        self.report_txt.write("Outgoing URLs:\n")
        self.report_txt.write(f"Total URLs extracted: {total_urls}\n")
        self.report_txt.write(f"# unique URLs extracted: {unique_urls}\n")
        self.report_txt.write(f"# unique URLs within News Site: {unique_within}\n")
        self.report_txt.write(f"# unique URLs outside News Site: {unique_outside}\n\n")

        status_counts = Counter(fetch_df["Status_Code"]) if not fetch_df.empty else Counter()
        self.report_txt.write("Status Codes:\n")
        for code, count in sorted(status_counts.items()):
            self.report_txt.write(f"{code}: {count}\n")
        self.report_txt.write("\n")

        size_bins = [0, 1024, 10240, 102400, 1048576, float("inf")]
        size_labels = ["<1KB", "1KB-<10KB", "10KB-<100KB", "100KB-<1MB", ">=1MB"]
        if not visit_df.empty:
            visit_df["Size Range"] = pd.cut(visit_df["Size (Bytes)"], bins=size_bins, labels=size_labels, right=False)
            size_counts = visit_df["Size Range"].value_counts().reindex(size_labels, fill_value=0)
        else:
            size_counts = pd.Series(0, index=size_labels)
        self.report_txt.write("File Sizes:\n")
        for label, count in size_counts.items():
            self.report_txt.write(f"{label}: {count}\n")
        self.report_txt.write("\n")

        content_counts = Counter(visit_df["Content-Type"]) if not visit_df.empty else Counter()
        self.report_txt.write("Content Types:\n")
        for ct, count in sorted(content_counts.items()):
            self.report_txt.write(f"{ct}: {count}\n")

        for f in [self.fetch_csv, self.visit_csv, self.urls_csv, self.report_txt]:
            if not f.closed:
                f.close()

        print(f"Fetch statistics for {self.NEWS_SITE_NAME} saved to report.")