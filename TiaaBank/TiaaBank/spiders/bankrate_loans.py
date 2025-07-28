import scrapy        #Import
import os
import csv
import json
from datetime import datetime, date
from scrapy.crawler import CrawlerProcess
class BankrateRatesSpider(scrapy.Spider):  #SPIDER CLASS DEFINITION
    name = "bankrate_rates"
    allowed_domains = ["bankrate.com"]
    start_urls = ["https://www.bankrate.com/mortgages/mortgage-rates/"]
    def __init__(self):       #INITIALIZATION (__init__ METHOD)
        super().__init__()
        # One permanent CSV + snapshot JSON
        self.csv_path = "bankrate_rates_history.csv"        # Master All-in-One CSV
        self.json_path = "bankrate_loans.json"     # Optional, overwritten daily
        self.fieldnames = ["Product", "Interest Rate", "APR", "timestamp"]
        self.scraped_data = []
    def parse(self, response):  # PARSE FUNCTION — START SCRAPING
        # Extract "Rates as of ..." date from the page
        raw_date = response.css('p.mb-0::text').re_first(r'Rates as of (.*)')   # Extract "Rates as of ..." date
        if raw_date:
            try:
                scraped_date = datetime.strptime(raw_date.strip(), "%A, %B %d, %Y at %I:%M %p").date()
            except Exception:
                self.logger.warning("Failed to parse date, using today instead.")
                scraped_date = date.today()
        else:
            self.logger.warning("No 'Rates as of' text found, using today instead.")
            scraped_date = date.today()
        today = date.today()      # SKIP OLD DATA (e.g., from cached pages)
        if scraped_date < today:
            self.logger.info(f":hourglass_flowing_sand: Skipping past data: {scraped_date} < {today}")
            return
        timestamp = scraped_date.isoformat()     # Set Timestamp Format for Output Records
        # Collect already saved entries to prevent duplicates
        existing_keys = set()
        if os.path.exists(self.csv_path):
            with open(self.csv_path, newline='', encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_keys.add((row["Product"], row["timestamp"]))
        # SCRAPE TABLE ROWS
        rows = response.css('div[aria-labelledby="purchase-0"] table tbody tr')
        for row in rows:
            product = row.css('th a::text').get(default='').strip()
            rate = row.css('td:nth-of-type(1)::text').get(default='').strip()
            apr = row.css('td:nth-of-type(2)::text').get(default='').strip()
            if not product or not rate or not apr:
                continue
            key = (product, timestamp)     # CHECK FOR DUPLICATES
            if key in existing_keys:
                continue  # skip previously saved row
            item = {                    # SAVE SCRAPED DATA
                "Product": product,
                "Interest Rate": rate,
                "APR": apr,
                "timestamp": timestamp
            }
            self.scraped_data.append(item)
            yield item
    def closed(self, reason):          # closed() — Saving Files After Spider Stops
        if not self.scraped_data:
            print(":warning: No new data scraped. Nothing saved.")
            return
        # Save Daily Snapshot to JSON (Optional)
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self.scraped_data, f, indent=2)
        print(f":white_check_mark: Snapshot JSON saved to {self.json_path}")
        # Append rows to master CSV file (no duplicates)
        write_header = not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerows(self.scraped_data)
        print(f":white_check_mark: Appended {len(self.scraped_data)} new row(s) to {self.csv_path}")
if __name__ == "__main__":                 # RUNNING THE SPIDER
    process = CrawlerProcess(settings={
        "USER_AGENT": "Mozilla/5.0",
        "LOG_LEVEL": "INFO"
    })
    process.crawl(BankrateRatesSpider)
    process.start()