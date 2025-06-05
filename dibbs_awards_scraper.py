# dibbs_awards_scraper.py

import argparse
import io
import json
import logging
import ssl
import sys
import textwrap
import time
from datetime import date, datetime, timedelta
from html.parser import HTMLParser

import requests
from util.bigquery_api import BigQuery
from util.configuration import Configuration
from util.csv import column_header_sanitize
from util.storage_api import Storage

# Constants
BUCKET = "fbc-awards"
DATASET = "DIBBS"
DIBBS_HOST = "https://www.dibbs.bsm.dla.mil/"
DIBBS2_HOST = "https://dibbs2.bsm.dla.mil/"
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "www.dibbs.bsm.dla.mil",
    "Pragma": "no-cache",
    "Referer": "https://www.dibbs.bsm.dla.mil/dodwarning.aspx?goto=/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}
SCHEMA = [
    {"name": "RowNum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "AwardBasicNumber", "type": "STRING", "mode": "Required"},
    {"name": "DeliveryOrder", "type": "STRING", "mode": "NULLABLE"},
    {"name": "DeliveryOrderCounter", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "Nsn", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Cage", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PostedDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "LastModPostingDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "AwardDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "TotalContactPrice", "type": "DECIMAL", "mode": "NULLABLE"},
    {"name": "PurchaseRequest", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Solicitation", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Nomenclature", "type": "STRING", "mode": "NULLABLE"},
    {
        "name": "Links",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "Award_Basic_Document", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Award_Basic_Package_View", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "Delivery_Order_Package_View",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {"name": "Delivery_Order_Document", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
]


# Set up logging
def setup_logging(verbose=False):
    """Configure logging for the application."""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    log_filename = f"dibbs_scraper_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)],
    )

    logger = logging.getLogger("dibbs_scraper")
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger


class FormParser(HTMLParser):
    """
    A custom HTMLParser class to extract form data, action, and method.
    """

    @staticmethod
    def parse(page):
        parser = FormParser()
        parser.feed(page)
        parser.close()
        form_data, form_action, form_method = parser.get_data()
        form_action = form_action.replace("./", "")
        return form_data, form_action, form_method

    def __init__(self):
        super().__init__()
        self.form_data = {}
        self.form_action = ""
        self.form_method = "get"  # Default method is GET
        self.parsing_form = False  # Flag to indicate if we are inside a form

    def handle_starttag(self, tag, attrs):
        """
        Handles the start tags of HTML elements. This method is called when the parser
        encounters a start tag (e.g., <form>, <input>).
        """
        if tag == "form":
            self.parsing_form = True
            attrs = dict(attrs)
            self.form_action = attrs["action"]
            self.form_method = attrs["method"].upper()
            self.form_data = {}
        elif self.parsing_form and tag == "input":
            attrs = dict(attrs)
            if "name" in attrs and "value" in attrs:
                self.form_data[attrs["name"]] = attrs["value"]

    def handle_endtag(self, tag):
        """Handles the end tags of HTML elements."""
        if tag == "form":
            self.parsing_form = False  # Exit form parsing mode

    def get_data(self):
        """Returns the extracted form data, action, and method."""
        return self.form_data, self.form_action, self.form_method


class AwdRecsParser(HTMLParser):
    """
    A custom HTMLParser to extract data from <td> tags within <tr> tags of class "AwdRecs".
    """

    @staticmethod
    def parse(page, logger=None):
        parser = AwdRecsParser(logger)
        parser.feed(page)
        parser.close()
        return parser.records, parser.get_data()

    def __init__(self, logger=None):
        super().__init__()
        self.logger = logger
        self.records = 0
        self.rows = []
        self.row = {}
        self.inside_tr = False
        self.inside_td = None  # stores id
        self.inside_span = False
        self.inside_count = False

    def handle_starttag(self, tag, attrs):
        """
        Handles the start tags of HTML elements.
        """
        if tag == "tr":
            if ("class", "BgWhite") in attrs or ("class", "BgSilver") in attrs:
                self.inside_tr = True
                self.row = {"Links": {}}
        elif tag == "span":
            if self.inside_td:
                self.inside_span = True
            elif self.inside_tr:
                attrs = dict(attrs)
                if "id" in attrs:
                    self.inside_td = dict(attrs)["id"].split("_lbl", 1)[-1]
            elif ("id", "ctl00_cph1_lblRecCount") in attrs:
                self.inside_count = True
        elif self.inside_td and tag == "a" and self.inside_td != "Cage":
            attrs = dict(attrs)
            self.row["Links"][
                column_header_sanitize(attrs["title"]).replace("Link_To_", "")
            ] = attrs["href"]

    def handle_endtag(self, tag):
        """
        Handles the end tags of HTML elements.
        """
        if tag == "tr" and self.inside_tr:
            self.inside_tr = False
            if self.row:
                self.row.setdefault("DeliveryOrder", "")
                self.rows.append(self.row)
        elif tag == "span":
            self.inside_count = False
            if self.inside_span:
                self.inside_span = False
            else:
                self.inside_td = None

    def handle_data(self, data):
        """
        Handles the text data within HTML elements.
        """
        if self.inside_td and not self.inside_span:
            data = data.strip()
            if data and self.inside_td == "TotalContactPrice":
                # Remove currency symbols and commas
                data = data.replace("$", "").replace(",", "")
                # Check if the remaining data is numeric
                try:
                    # Try to convert to float to validate it's numeric
                    float(data)
                except ValueError:
                    # If it's not numeric (e.g., "See Award Doc"), don't set the field at all
                    # This will result in NULL in BigQuery rather than an empty string
                    if self.logger:
                        self.logger.debug(
                            f"Non-numeric TotalContactPrice value: '{data}' - skipping field"
                        )
                    return  # Skip setting this field entirely
            elif data and self.inside_td in (
                "LastModPostingDate",
                "AwardDate",
                "PostedDate",
            ):
                try:
                    # Log the original format for debugging
                    if self.logger:
                        self.logger.debug(f"Original {self.inside_td} format: '{data}'")

                    # Try to parse MM-DD-YYYY format
                    parts = data.split("-")
                    if len(parts) == 3:
                        month, day, year = parts
                        # Validate the parts
                        if len(month) == 2 and len(day) == 2 and len(year) == 4:
                            data = f"{year}-{month}-{day}"
                            if self.logger:
                                self.logger.debug(
                                    f"Converted {self.inside_td} to: '{data}'"
                                )
                        else:
                            raise ValueError(
                                f"Invalid date part lengths: month={len(month)}, day={len(day)}, year={len(year)}"
                            )
                    else:
                        raise ValueError(f"Expected 3 parts, got {len(parts)}")
                except Exception as e:
                    # Log problematic dates
                    if self.logger:
                        self.logger.warning(
                            f"Date parsing error for {self.inside_td}: '{data}' - {str(e)} - skipping field"
                        )
                    return  # Skip setting this field entirely

            self.row.setdefault(self.inside_td, "")
            self.row[self.inside_td] += data
        elif self.inside_count:
            try:
                self.records = int(
                    data.strip()
                )  # loads twice (span + bold), second time in strong (exploit side effect)
            except ValueError:
                pass


def get_request_headers(headers=None):
    """
    Combines the default headers with any additional headers provided for a request.
    """
    req_headers = HEADERS.copy()  # Start with the default headers
    if headers:
        req_headers.update(headers)  # Update with any specific headers
    return req_headers


def inspect_cookies(session, logger):
    """
    Logs information about cookies for debug.
    """
    logger.debug("Cookies in session:")
    for cookie in session.cookies:
        logger.debug(f"Cookie: {cookie.name}, Domain: {cookie.domain}")


def dibbs_session(host, logger, verify=True, max_retries=3):
    """
    The first page of dibbs sets a cookie which is triggered via a form submit.
    This is required to access all other pages.

    The steps are:
      - Pass believable headers.
      - Load the start page (with OK form).
      - Parse form out of page.
      - Submit form to set cookie.
      - Store cookies in Session class which has get/post wrappers.
      - Return session for future use.
    """
    session = requests.Session()

    # Set up retry mechanism
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempt {attempt+1} to establish DIBBS session with {host}")

            # Load first page
            page_response = session.request(
                "GET", url=host + "dodwarning.aspx?goto=/", verify=verify, timeout=30
            )
            page_response.raise_for_status()
            page = page_response.content.decode("utf-8")

            # Parse form (form must match all parameters exactly on page)
            try:
                form_data, form_action, form_method = FormParser().parse(page)
                logger.debug(f"Form parsed successfully. Action: {form_action}")
            except Exception as e:
                logger.error(f"Form parsing error: {str(e)}")
                raise

            # Submit form to set cookie (response doesn't matter)
            form_response = session.request(
                "POST",
                url=host + form_action,
                data=form_data,
                verify=verify,
                timeout=30,
            )
            form_response.raise_for_status()

            if logger.isEnabledFor(logging.DEBUG):
                inspect_cookies(session, logger)

            logger.info(f"Successfully established DIBBS session with {host}")
            return session

        except requests.RequestException as e:
            logger.warning(f"Request failed on attempt {attempt+1}: {str(e)}")
            if attempt == max_retries - 1:
                logger.error(
                    f"Failed to establish DIBBS session after {max_retries} attempts"
                )
                raise
            time.sleep(2**attempt)  # Exponential backoff


def dibbs_awards_table(day):
    """Generate the table name for a given day."""
    month, day, year = day.split("-")
    return f"RAW_{year}_{month}_{day}"


def dibbs_awards_save(config, day, rows, logger):
    """
    Write the daily records to BigQuery.
    """
    table = dibbs_awards_table(day)
    logger.info(f"Saving {len(rows)} records to table {DATASET}.{table}")

    try:
        result = BigQuery(config, "service").json_to_table(
            project_id=config.project,
            dataset_id=DATASET,
            table_id=table,
            json_data=rows,
            schema=SCHEMA,  # SCHEMA
        )
        logger.info(f"Successfully saved data to {DATASET}.{table}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to BigQuery: {str(e)}")
        return False


def dibbs_awards_clean(config, day, logger):
    """
    Write the clean version to a table (can be called independently to redo logic).
    """
    raw_table = dibbs_awards_table(day)
    clean_table = raw_table.replace("RAW", "CLEAN")

    logger.info(f"Cleaning data from {DATASET}.{raw_table} to {DATASET}.{clean_table}")

    # First check if RAW table exists
    if not BigQuery(config, "service").table_exists(
        project_id=config.project, dataset_id=DATASET, table_id=raw_table
    ):
        logger.error(f"RAW table {DATASET}.{raw_table} does not exist. Cannot clean.")
        return False

    try:
        query = f"""
            SELECT
                AwardBasicNumber, # key
                DeliveryOrder, # key
                Cage,
                AwardDate,
                PostedDate,
                TotalContactPrice,
                LastModPostingDate,
                Links,
                ARRAY_AGG(STRUCT(
                    Nsn,
                    Nomenclature,
                    PurchaseRequest,
                    Solicitation
                )) AS Parts

            FROM `{config.project}.{DATASET}.{raw_table}`
            GROUP BY 1,2,3,4,5,6,7,8
        """

        result = BigQuery(config, "service").query_to_table(
            project_id=config.project,
            dataset_id=DATASET,
            table_id=clean_table,
            query=query,
            disposition="WRITE_TRUNCATE",
        )

        logger.info(f"Successfully cleaned data to {DATASET}.{clean_table}")
        return True
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")
        return False


def dibbs_awards_storage(config, rows, logger):
    """
    Session against content server and download files to storage.
    """
    logger.info(f"Starting PDF download for {len(rows)} rows")

    # Track statistics
    stats = {"total_links": 0, "already_exists": 0, "downloaded": 0, "failed": 0}

    try:
        done = set()
        session = dibbs_session(DIBBS2_HOST, logger, verify=False)

        for row_idx, row in enumerate(rows):
            if row_idx % 10 == 0:
                logger.debug(f"Processing PDFs for row {row_idx+1}/{len(rows)}")

            key = (row["AwardBasicNumber"], row["DeliveryOrder"])
            if key not in done:
                done.add(key)
                for title, url in row["Links"].items():
                    stats["total_links"] += 1

                    if url.endswith(".PDF"):
                        filename = f"{DATASET}/{row['AwardBasicNumber']}_{row['DeliveryOrder']}_{title}.pdf"
                        if Storage(config, "service").object_exists(
                            bucket=BUCKET, filename=filename
                        ):
                            logger.debug(f"File already exists: {BUCKET}/{filename}")
                            stats["already_exists"] += 1
                        else:
                            try:
                                logger.info(f"Downloading PDF: {BUCKET}/{filename}")
                                data = session.request(
                                    "GET",
                                    verify=False,
                                    url=url,
                                    headers={
                                        "Host": "dibbs2.bsm.dla.mil",
                                        "Sec-Fetch-Mode": "navigate",
                                        "Sec-Fetch-Site": "none",
                                        "Sec-Fetch-User": "?1",
                                    },
                                    timeout=60,
                                ).content

                                Storage(config, "service").object_put(
                                    bucket=BUCKET,
                                    filename=filename,
                                    data=io.BytesIO(data),
                                    mimetype="application/pdf",
                                )
                                stats["downloaded"] += 1
                                logger.info(
                                    f"Successfully uploaded: {BUCKET}/{filename}"
                                )
                            except Exception as e:
                                stats["failed"] += 1
                                logger.error(
                                    f"Error downloading/uploading PDF {url}: {str(e)}"
                                )
                            finally:
                                time.sleep(1)  # Add 1-second delay after each attempt

        logger.info(
            f"PDF processing complete: {stats['downloaded']} downloaded, "
            + f"{stats['already_exists']} already existed, {stats['failed']} failed"
        )
        return stats
    except Exception as e:
        logger.error(f"Error in storage processing: {str(e)}")
        return stats


def dibbs_awards(config, day, logger):
    """
    Pagination code on the page calls a javascript post back:

    <td><a href="javascript:__doPostBack(&#39;ctl00$cph1$grdAwardSearch&#39;,&#39;Page$2&#39;)">2</a></td>
    <td><a href="javascript:__doPostBack(&#39;ctl00$cph1$grdAwardSearch&#39;,&#39;Page$9&#39;)">9</a></td>

    Which calls this code to set __EVENTTARGET and __EVENTARGUMENT:

    function __doPostBack(eventTarget, eventArgument) {
      if (!theForm.onsubmit || (theForm.onsubmit() != false)) {
          theForm.__EVENTTARGET.value = eventTarget;
          theForm.__EVENTARGUMENT.value = eventArgument;
          theForm.submit();
      }
    }

    For example Page 2 is:
    <a href="javascript:__doPostBack(&#39;ctl00$cph1$grdAwardSearch&#39;,&#39;Page$2&#39;)">2</a></td>
    __doPostBack('ctl00$cph1$grdAwardSearch', 'Page$2')
    __EVENTTARGET = 'ctl00$cph1$grdAwardSearch'
    __EVENTARGUMENT = 'Page$2'

    For example Page 9 is:
    <td><a href="javascript:__doPostBack(&#39;ctl00$cph1$grdAwardSearch&#39;,&#39;Page$9&#39;)">9</a></td>
    __doPostBack('ctl00$cph1$grdAwardSearch', 'Page$9')
    __EVENTTARGET = 'ctl00$cph1$grdAwardSearch'
    __EVENTARGUMENT = 'Page$9'

    Process:
      - get page.
      - pull form.
      - loop submit forms.
      - scrape documents.
    """
    number = 1
    rows = []
    stats = {"success": False, "pages": 0, "rows": 0, "expected_rows": 0}

    logger.info(f"Processing DIBBS awards for date: {day}")

    try:
        # Activate dibbs access
        session_dibbs = dibbs_session(DIBBS_HOST, logger)

        logger.info(f"Starting with page {number} for date {day}")

        # Load first page
        form_action = (
            f"{DIBBS_HOST}Awards/AwdRecs.aspx?Category=post&TypeSrch=cq&Value={day}"
        )
        page_response = session_dibbs.request("GET", form_action, timeout=30)
        page_response.raise_for_status()
        page = page_response.content.decode("utf-8")

        form_data, _, form_method = FormParser.parse(page)  # No logger parameter
        count, rows_more = AwdRecsParser.parse(page, logger)  # Pass logger here
        rows.extend(rows_more)
        stats["expected_rows"] = count
        stats["pages"] = 1

        logger.info(
            f"Found {count} total records across multiple pages. Retrieved {len(rows_more)} from page 1."
        )

        # Loop additional pages
        max_pages = 50  # Safety limit
        while len(rows) < count and len(rows_more) > 0 and number < max_pages:
            number += 1
            logger.info(
                f"Processing page {number} ({len(rows)} of {count} records retrieved so far)"
            )
            stats["pages"] = number

            form_data["__EVENTTARGET"] = "ctl00$cph1$grdAwardSearch"
            form_data["__EVENTARGUMENT"] = f"Page${number}"

            # Default select fields
            form_data["ctl00$ddlNavigation"] = ""
            form_data["ctl00$ddlGotoDB"] = ""
            form_data["ctl00$txtValue"] = ""

            # Remove buttons if they exist
            form_data.pop("ctl00$butNavigation", None)
            form_data.pop("ctl00$butDbSearch", None)

            try:
                page_response = session_dibbs.request(
                    "POST", url=form_action, data=form_data, timeout=60
                )
                page_response.raise_for_status()
                page = page_response.content.decode("utf-8")

                form_data, _, form_method = FormParser.parse(
                    page
                )  # No logger parameter
                _, rows_more = AwdRecsParser.parse(page, logger)  # Pass logger here

                logger.info(f"Retrieved {len(rows_more)} records from page {number}")
                rows.extend(rows_more)
            except Exception as e:
                logger.error(f"Error processing page {number}: {str(e)}")
                break

        stats["rows"] = len(rows)
        logger.info(f"Retrieved a total of {len(rows)} records out of {count} expected")

        # Save all rows and PDFs
        if rows:
            save_success = dibbs_awards_save(config, day, rows, logger)
            clean_success = dibbs_awards_clean(config, day, logger)
            storage_stats = dibbs_awards_storage(config, rows, logger)

            stats["success"] = save_success and clean_success
            stats.update(storage_stats)

            if stats["success"]:
                logger.info(f"Successfully processed awards for {day}")
            else:
                logger.warning(f"Partial success processing awards for {day}")
        else:
            logger.warning(f"No rows retrieved for {day}")

        return stats
    except Exception as e:
        logger.error(f"Error processing awards for {day}: {str(e)}")
        stats["error"] = str(e)
        return stats


def backfill_dibbs_awards(config, start_date, end_date, logger):
    """
    Backfill DIBBS awards data for a date range.

    Args:
        config: Configuration object
        start_date: Start date in datetime.date format
        end_date: End date in datetime.date format
        logger: Logger object
    """
    logger.info(f"Starting backfill from {start_date} to {end_date}")

    results = {
        "total_days": 0,
        "successful_days": 0,
        "failed_days": 0,
        "skipped_days": 0,
        "details": {},
    }

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%m-%d-%Y")
        logger.info(f"Processing date: {date_str}")
        results["total_days"] += 1

        try:
            stats = dibbs_awards(config, date_str, logger)

            if stats["success"]:
                results["successful_days"] += 1
                logger.info(f"Successfully processed {date_str}")
            else:
                results["failed_days"] += 1
                logger.warning(f"Failed to fully process {date_str}")

            results["details"][date_str] = stats

        except Exception as e:
            results["failed_days"] += 1
            logger.error(f"Error processing {date_str}: {str(e)}")
            results["details"][date_str] = {"success": False, "error": str(e)}

        current_date += timedelta(days=1)

    success_rate = (
        (results["successful_days"] / results["total_days"]) * 100
        if results["total_days"] > 0
        else 0
    )
    logger.info(
        f"Backfill complete: {results['successful_days']}/{results['total_days']} days successful ({success_rate:.1f}%)"
    )

    return results


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """\
        Use this to download DIBBS awards (and RFQs in future) to storage and BigQuery.
        This script will download into dataset called DIBBS.
        It downloads all the daily records and overwrites the table for the day.
        If it runs every 30 minutes, it will re-create the table (which is OK).

        DIBBS
         - https://www.dibbs.bsm.dla.mil/dodwarning.aspx?goto=/

        RFQ
         - https://www.dibbs.bsm.dla.mil/RFQ/RFQDates.aspx?category=recent
         - https://www.dibbs.bsm.dla.mil/RFQ/RfqDates.aspx?category=issue
         - https://www.dibbs.bsm.dla.mil/RFQ/RfqRecs.aspx?category=issue&TypeSrch=dt&Value=04-29-2025
         - https://dibbs2.bsm.dla.mil/Downloads/RFQ/8/SPE1C125Q0278.PDF
         - https://dibbs2.bsm.dla.mil/Downloads/RFQ/9/SPE1C125Q0299.PDF
         - https://dibbs2.bsm.dla.mil/Downloads/RFQ/0/SPE1C125Q0300.PDF
         - https://dibbs2.bsm.dla.mil/Downloads/RFQ/0/SPE1C125T1980.PDF

        Award
         - https://www.dibbs.bsm.dla.mil/Awards/AwdDates.aspx?category=awddt
         - https://www.dibbs.bsm.dla.mil/Awards/AwdRecs.aspx?Category=awddt&TypeSrch=cq&Value=04-29-2025
         - https://dibbs2.bsm.dla.mil/Downloads/Awards/23SEP21/SP330021D0021.PDF
         - https://dibbs2.bsm.dla.mil/Downloads/Awards/06MAY24/SP330024D0007.PDF

        Example:
          python dibbs_awards_scraper.py -s argon-surf-426917-k8-f830e0025aa9.json -p argon-surf-426917-k8
          python dibbs_awards_scraper.py -s argon-surf-426917-k8-f830e0025aa9.json -p argon-surf-426917-k8 --day [MM-DD-YYYY to download and clean]
          python dibbs_awards_scraper.py -s argon-surf-426917-k8-f830e0025aa9.json -p argon-surf-426917-k8 --clean [MM-DD-YYYY to clean table only]
          python dibbs_awards_scraper.py -s argon-surf-426917-k8-f830e0025aa9.json -p argon-surf-426917-k8 --backfill 04-25-2025,04-27-2025
          python dibbs_awards_scraper.py -s argon-surf-426917-k8-f830e0025aa9.json -p argon-surf-426917-k8

          Normal daily operation (yesterday and today)
          python dibbs_awards_scraper.py -s service.json -p argon-surf-426917-k8

          Process a specific day
          python dibbs_awards_scraper.py -s service.json -p argon-surf-426917-k8 --day 05-01-2025

          Clean a specific day's data
          python dibbs_awards_scraper.py -s service.json -p argon-surf-426917-k8 --clean 05-01-2025

          Backfill a date range
          python dibbs_awards_scraper.py -s service.json -p argon-surf-426917-k8 --backfill 04-01-2025,04-03-2025

        """
        ),
    )

    parser.add_argument(
        "--project", "-p", help="Cloud ID of Google Cloud Project.", default=None
    )
    parser.add_argument(
        "--service", "-s", help="Path to SERVICE credentials json file.", default=None
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Print all the steps as they happen.",
        action="store_true",
    )

    parser.add_argument(
        "--day", "-d", help="Date to load in MM-DD-YYYY format.", default=None
    )
    parser.add_argument(
        "--clean", "-c", help="Date to clean in MM-DD-YYYY format.", default=None
    )
    parser.add_argument(
        "--backfill",
        "-b",
        help="Date range to backfill in MM-DD-YYYY,MM-DD-YYYY format.",
        default=None,
    )
    parser.add_argument(
        "--test",
        "-t",
        help="Run test (requires AwdRecs.aspx file).",
        action="store_true",
    )

    args = parser.parse_args()

    # Set up logging
    logger = setup_logging(args.verbose)

    # Set up configuration
    config = Configuration(
        project=args.project, service=args.service, verbose=args.verbose
    )

    # Log start of script execution
    logger.info("=" * 80)
    logger.info("Starting DIBBS Award Scraper")
    logger.info(f"Project: {config.project}")

    start_time = datetime.now()

    try:
        if args.test:
            logger.info("Running in test mode")
            try:
                with open("AwdRecs.aspx", "r") as file:
                    count, rows = AwdRecsParser.parse(
                        file.read(), logger
                    )  # Pass logger here
                    logger.info(f"TEST: Parsed {count} records from test file")
                    logger.debug(
                        f"TEST: First record: {json.dumps(rows[0] if rows else {}, indent=2)}"
                    )
            except FileNotFoundError:
                logger.error("Test file 'AwdRecs.aspx' not found")
                return 1

        # Handle backfill
        elif args.backfill:
            try:
                date_parts = args.backfill.split(",")
                if len(date_parts) != 2:
                    logger.error(
                        "Backfill requires two dates in format MM-DD-YYYY,MM-DD-YYYY"
                    )
                    return 1

                start_date = datetime.strptime(date_parts[0], "%m-%d-%Y").date()
                end_date = datetime.strptime(date_parts[1], "%m-%d-%Y").date()

                if start_date > end_date:
                    logger.error(
                        f"Start date {start_date} is after end date {end_date}"
                    )
                    return 1

                results = backfill_dibbs_awards(config, start_date, end_date, logger)
                logger.info(
                    f"Backfill results: {results['successful_days']}/{results['total_days']} successful"
                )
            except ValueError as e:
                logger.error(f"Invalid date format: {str(e)}")
                return 1

        # Handle single day
        elif args.day:
            logger.info(f"Processing single day: {args.day}")
            try:
                # Validate date format
                datetime.strptime(args.day, "%m-%d-%Y")
                stats = dibbs_awards(config, args.day, logger)

                if stats["success"]:
                    logger.info(f"Successfully processed day {args.day}")
                else:
                    logger.warning(f"Issues processing day {args.day}")
                    return 1
            except ValueError:
                logger.error(f"Invalid date format: {args.day}. Expected MM-DD-YYYY")
                return 1

        # Handle clean-only
        elif args.clean:
            logger.info(f"Cleaning data for day: {args.clean}")
            try:
                # Validate date format
                datetime.strptime(args.clean, "%m-%d-%Y")
                success = dibbs_awards_clean(config, args.clean, logger)

                if success:
                    logger.info(f"Successfully cleaned data for {args.clean}")
                else:
                    logger.warning(f"Issues cleaning data for {args.clean}")
                    return 1
            except ValueError:
                logger.error(f"Invalid date format: {args.clean}. Expected MM-DD-YYYY")
                return 1

        # Default mode - run yesterday and today
        else:
            logger.info("Running in default mode (yesterday and today)")
            success = True

            # Process yesterday if today's table doesn't exist yet
            today = date.today().strftime("%m-%d-%Y")
            today_table = dibbs_awards_table(today)

            if not BigQuery(config, "service").table_exists(
                project_id=config.project, dataset_id=DATASET, table_id=today_table
            ):
                yesterday = (date.today() - timedelta(days=1)).strftime("%m-%d-%Y")
                logger.info(f"Processing yesterday's data: {yesterday}")

                yesterday_stats = dibbs_awards(config, yesterday, logger)
                if not yesterday_stats["success"]:
                    logger.warning(f"Issues processing yesterday's data ({yesterday})")
                    success = False

            # Always process today
            logger.info(f"Processing today's data: {today}")
            today_stats = dibbs_awards(config, today, logger)

            if not today_stats["success"]:
                logger.warning(f"Issues processing today's data ({today})")
                success = False

            if not success:
                return 1

        # Log successful completion
        runtime = datetime.now() - start_time
        logger.info(f"Script completed successfully in {runtime}")
        return 0

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
