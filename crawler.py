#!/usr/bin/env python3
"""
DIBBS Historical Data Crawler - Crawls all available dates and downloads complete datasets
"""

import argparse
import json
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
import requests

from util.storage_api import Storage
from util.configuration import Configuration
from util.bigquery_api import BigQuery

# Import existing modules
from solicitations import dibbs_solicitations, dibbs_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'
BUCKET = 'fbc-requests'
DATASET = 'DIBBS'


class RFQDatesParser(HTMLParser):
    """Parse the RFQ dates page to extract all available dates and download links"""
    def __init__(self):
        super().__init__()
        self.dates = []
        self.current_row = {}
        self.in_row = False
        self.in_cell = False
        self.current_cell_type = None
        self.cell_count = 0
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == 'tr' and attrs_dict.get('class') in ['BgWhite', 'BgSilver']:
            self.in_row = True
            self.current_row = {}
            self.cell_count = 0
            
        elif tag == 'td' and self.in_row:
            self.in_cell = True
            self.cell_count += 1
            
        elif tag == 'a' and self.in_cell:
            href = attrs_dict.get('href', '')
            
            if self.cell_count == 1:  # Post Date column
                # Extract date from the RFQ link
                match = re.search(r'Value=(\d{2}-\d{2}-\d{4})', href)
                if match:
                    self.current_row['date'] = match.group(1)
                    self.current_row['rfq_url'] = href
                    
            elif self.cell_count == 2:  # Zip File Name column
                if 'ca' in href:
                    self.current_row['ca_url'] = href
                    
            elif self.cell_count == 3:  # Index File Name column
                if 'in' in href:
                    self.current_row['in_url'] = href
                    
            elif self.cell_count == 4:  # Batch Quote File Name column
                if 'bq' in href:
                    self.current_row['bq_url'] = href
    
    def handle_endtag(self, tag):
        if tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row and 'date' in self.current_row:
                self.dates.append(self.current_row)
                
        elif tag == 'td' and self.in_cell:
            self.in_cell = False


class DIBBSHistoricalCrawler:
    def __init__(self, config, start_date=None, end_date=None, skip_existing=True):
        self.config = config
        self.storage = Storage(config, "service")
        self.bq = BigQuery(config, "service")
        self.start_date = start_date
        self.end_date = end_date
        self.skip_existing = skip_existing
        
        # Create sessions
        self.session_dibbs = dibbs_session(DIBBS_HOST)
        self.session_dibbs2 = dibbs_session(DIBBS2_HOST, verify=False)
        
        # Track progress
        self.progress = {
            'dates_found': 0,
            'dates_processed': 0,
            'dates_skipped': 0,
            'files_downloaded': 0,
            'files_uploaded': 0,
            'errors': []
        }
        
    def crawl_available_dates(self) -> List[Dict]:
        """Crawl the RFQ dates page to get all available dates"""
        logger.info("Crawling DIBBS RFQ dates page...")
        
        all_dates = []
        page_num = 1
        
        while True:
            try:
                # Load dates page
                url = f"{DIBBS_HOST}RFQ/RfqDates.aspx?category=recent"
                
                if page_num == 1:
                    response = self.session_dibbs.get(url)
                else:
                    # Handle pagination
                    # First get the page to get form data
                    response = self.session_dibbs.get(url)
                    page_content = response.content.decode('utf-8')
                    
                    # Parse form data
                    from solicitations import FormParser
                    form_data, _, _ = FormParser().parse(page_content)
                    
                    # Set pagination parameters
                    form_data['__EVENTTARGET'] = 'ctl00$cph1$rep1'
                    form_data['__EVENTARGUMENT'] = f'Page${page_num}'
                    
                    # Submit form for next page
                    response = self.session_dibbs.post(url, data=form_data)
                
                page_content = response.content.decode('utf-8')
                
                # Parse dates from page
                parser = RFQDatesParser()
                parser.feed(page_content)
                
                if not parser.dates:
                    logger.info(f"No more dates found on page {page_num}")
                    break
                    
                logger.info(f"Found {len(parser.dates)} dates on page {page_num}")
                all_dates.extend(parser.dates)
                
                # Check if we have more pages
                if 'Page$' + str(page_num + 1) not in page_content:
                    break
                    
                page_num += 1
                time.sleep(1)  # Be respectful
                
            except Exception as e:
                logger.error(f"Error crawling page {page_num}: {str(e)}")
                break
        
        # Filter by date range if specified
        if self.start_date or self.end_date:
            filtered_dates = []
            for date_info in all_dates:
                date_obj = datetime.strptime(date_info['date'], '%m-%d-%Y')
                
                if self.start_date and date_obj < self.start_date:
                    continue
                if self.end_date and date_obj > self.end_date:
                    continue
                    
                filtered_dates.append(date_info)
            
            logger.info(f"Filtered to {len(filtered_dates)} dates within range")
            return filtered_dates
        
        return all_dates
    
    def check_date_exists(self, date_str: str) -> bool:
        """Check if date already exists in BigQuery"""
        if not self.skip_existing:
            return False
            
        table_name = f"SOLICITATIONS_{date_str.replace('-', '_')}"
        exists = self.bq.table_exists(self.config.project, DATASET, table_name)
        
        if exists:
            logger.info(f"Table {DATASET}.{table_name} already exists, checking if complete...")
            
            # Check if table has data
            query = f"SELECT COUNT(*) as count FROM `{self.config.project}.{DATASET}.{table_name}`"
            try:
                result = self.bq.query(query)
                if result and result[0]['count'] > 0:
                    return True
            except:
                pass
                
        return False
    
    def download_file(self, url: str, local_path: str, session=None) -> bool:
        """Download a file with the appropriate session"""
        if session is None:
            session = self.session_dibbs2 if 'dibbs2' in url else self.session_dibbs
            
        try:
            response = session.get(url, timeout=60, stream=True, verify=False)
            response.raise_for_status()
            
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        
            self.progress['files_downloaded'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            self.progress['errors'].append(f"Download failed: {url}")
            return False
    
    def upload_to_storage(self, local_path: str, gcs_path: str) -> bool:
        """Upload file to Google Cloud Storage"""
        try:
            with open(local_path, 'rb') as f:
                self.storage.object_put(
                    bucket=BUCKET,
                    filename=gcs_path,
                    data=f,
                    mimetype='application/octet-stream'
                )
            
            self.progress['files_uploaded'] += 1
            logger.debug(f"Uploaded to gs://{BUCKET}/{gcs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {str(e)}")
            self.progress['errors'].append(f"Upload failed: {gcs_path}")
            return False
    
    def process_date(self, date_info: Dict, temp_dir: str) -> Dict:
        """Process all data for a single date"""
        date_str = date_info['date']
        date_code = datetime.strptime(date_str, '%m-%d-%Y').strftime('%y%m%d')
        
        logger.info(f"\nProcessing {date_str}...")
        
        results = {
            'date': date_str,
            'success': False,
            'solicitations': {},
            'files': {
                'in': False,
                'bq': False,
                'as': False,
                'pdfs': 0
            }
        }
        
        # Create temp directory for this date
        date_temp_dir = os.path.join(temp_dir, date_code)
        os.makedirs(date_temp_dir, exist_ok=True)
        
        try:
            # Step 1: Web scrape solicitations
            logger.info(f"  Scraping solicitations for {date_str}")
            solicitations = dibbs_solicitations(self.config, date_str, test_mode=False)
            results['solicitations'] = len(solicitations)
            
            # Build solicitation -> PR mapping
            sol_pr_map = {}
            for sol in solicitations:
                sol_num = sol.get('solicitation', '').strip()
                pr_num = sol.get('pr_number', '').strip()
                
                if sol_num and pr_num:
                    if sol_num not in sol_pr_map:
                        sol_pr_map[sol_num] = set()
                    sol_pr_map[sol_num].add(pr_num)
            
            # Step 2: Download IN file
            if 'in_url' in date_info:
                logger.info(f"  Downloading IN file")
                in_path = os.path.join(date_temp_dir, f'in{date_code}.txt')
                
                if self.download_file(date_info['in_url'], in_path):
                    results['files']['in'] = True
                    
                    # Upload to each solicitation/PR folder
                    for sol_num, pr_nums in sol_pr_map.items():
                        for pr_num in pr_nums:
                            gcs_path = f"{DATASET}/{sol_num}/{pr_num}/in{date_code}.txt"
                            self.upload_to_storage(in_path, gcs_path)
            
            # Step 3: Download and extract BQ file
            if 'bq_url' in date_info:
                logger.info(f"  Downloading BQ file")
                bq_zip_path = os.path.join(date_temp_dir, f'bq{date_code}.zip')
                
                if self.download_file(date_info['bq_url'], bq_zip_path):
                    results['files']['bq'] = True
                    
                    # Extract files
                    try:
                        with zipfile.ZipFile(bq_zip_path, 'r') as zf:
                            zf.extractall(date_temp_dir)
                            
                        # Upload BQ and AS files to each solicitation/PR folder
                        for sol_num, pr_nums in sol_pr_map.items():
                            for pr_num in pr_nums:
                                # Upload BQ file
                                bq_txt = os.path.join(date_temp_dir, f'bq{date_code}.txt')
                                if os.path.exists(bq_txt):
                                    gcs_path = f"{DATASET}/{sol_num}/{pr_num}/bq{date_code}.txt"
                                    self.upload_to_storage(bq_txt, gcs_path)
                                
                                # Upload AS file
                                as_txt = os.path.join(date_temp_dir, f'as{date_code}.txt')
                                if os.path.exists(as_txt):
                                    results['files']['as'] = True
                                    gcs_path = f"{DATASET}/{sol_num}/{pr_num}/as{date_code}.txt"
                                    self.upload_to_storage(as_txt, gcs_path)
                                    
                    except Exception as e:
                        logger.error(f"Error extracting BQ zip: {str(e)}")
            
            # Step 4: Download CA file and extract PDFs
            if 'ca_url' in date_info:
                logger.info(f"  Downloading CA file for PDFs")
                ca_zip_path = os.path.join(date_temp_dir, f'ca{date_code}.zip')
                
                if self.download_file(date_info['ca_url'], ca_zip_path):
                    try:
                        with zipfile.ZipFile(ca_zip_path, 'r') as zf:
                            for filename in zf.namelist():
                                if filename.lower().endswith('.pdf'):
                                    # Extract PDF
                                    zf.extract(filename, date_temp_dir)
                                    pdf_path = os.path.join(date_temp_dir, filename)
                                    
                                    # Extract solicitation number from filename
                                    match = re.match(r'(SPE\w+\d+)', filename, re.IGNORECASE)
                                    if match:
                                        sol_num = match.group(1).upper()
                                        gcs_path = f"{DATASET}/{sol_num}/{filename}"
                                        
                                        if self.upload_to_storage(pdf_path, gcs_path):
                                            results['files']['pdfs'] += 1
                                    
                    except Exception as e:
                        logger.error(f"Error extracting CA zip: {str(e)}")
            
            # Step 5: Download individual solicitation PDFs from web scrape
            for sol in solicitations:
                sol_num = sol.get('solicitation', '')
                sol_url = sol.get('solicitation_url', '')
                
                if sol_url and sol_url.endswith('.PDF'):
                    # Extract the PDF number from URL for proper filename
                    match = re.search(r'/(\d+)/(SPE\w+\d+)\.PDF', sol_url, re.IGNORECASE)
                    if match:
                        filename = f"{match.group(2)}.PDF"
                        pdf_path = os.path.join(date_temp_dir, filename)
                        
                        if self.download_file(sol_url, pdf_path, self.session_dibbs2):
                            gcs_path = f"{DATASET}/{sol_num}/{filename}"
                            
                            if self.upload_to_storage(pdf_path, gcs_path):
                                results['files']['pdfs'] += 1
            
            # Save solicitation data for BigQuery
            sol_json_path = os.path.join(date_temp_dir, f'solicitations_{date_code}.json')
            with open(sol_json_path, 'w') as f:
                json.dump(solicitations, f, indent=2)
            
            results['success'] = True
            
        except Exception as e:
            logger.error(f"Error processing {date_str}: {str(e)}")
            self.progress['errors'].append(f"Processing failed for {date_str}: {str(e)}")
            
        return results
    
    def run(self):
        """Run the complete historical crawl"""
        start_time = datetime.now()
        
        # Create temp directory
        temp_dir = os.path.join('.', 'dibbs_historical_temp')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Step 1: Crawl available dates
        logger.info("=" * 80)
        logger.info("DIBBS Historical Data Crawler")
        logger.info("=" * 80)
        
        available_dates = self.crawl_available_dates()
        self.progress['dates_found'] = len(available_dates)
        
        logger.info(f"Found {len(available_dates)} dates to process")
        
        # Step 2: Process each date
        results_file = os.path.join(temp_dir, 'crawl_results.json')
        results = []
        
        for i, date_info in enumerate(available_dates):
            date_str = date_info['date']
            
            # Check if already processed
            if self.check_date_exists(date_str):
                logger.info(f"Skipping {date_str} - already processed")
                self.progress['dates_skipped'] += 1
                continue
            
            logger.info(f"\nProcessing date {i+1}/{len(available_dates)}: {date_str}")
            
            # Process the date
            result = self.process_date(date_info, temp_dir)
            results.append(result)
            
            if result['success']:
                self.progress['dates_processed'] += 1
            
            # Save progress
            with open(results_file, 'w') as f:
                json.dump({
                    'progress': self.progress,
                    'results': results
                }, f, indent=2)
            
            # Rate limiting
            time.sleep(2)
        
        # Final summary
        elapsed = datetime.now() - start_time
        
        print("\n" + "=" * 80)
        print("CRAWL COMPLETE")
        print("=" * 80)
        print(f"Total time: {elapsed}")
        print(f"Dates found: {self.progress['dates_found']}")
        print(f"Dates processed: {self.progress['dates_processed']}")
        print(f"Dates skipped: {self.progress['dates_skipped']}")
        print(f"Files downloaded: {self.progress['files_downloaded']}")
        print(f"Files uploaded: {self.progress['files_uploaded']}")
        print(f"Errors: {len(self.progress['errors'])}")
        
        if self.progress['errors']:
            print("\nErrors encountered:")
            for error in self.progress['errors'][:10]:
                print(f"  - {error}")
            if len(self.progress['errors']) > 10:
                print(f"  ... and {len(self.progress['errors']) - 10} more")
        
        print(f"\nResults saved to: {results_file}")
        
        return results


def main():
    parser = argparse.ArgumentParser(
        description='DIBBS Historical Data Crawler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Crawl all available dates
  python dibbs_historical_crawler.py -p PROJECT_ID -s service.json
  
  # Crawl specific date range
  python dibbs_historical_crawler.py -p PROJECT_ID -s service.json --start-date 2024-01-01 --end-date 2024-12-31
  
  # Force re-process existing dates
  python dibbs_historical_crawler.py -p PROJECT_ID -s service.json --no-skip-existing
        """
    )
    
    parser.add_argument('--project', '-p', required=True, help='Google Cloud Project ID')
    parser.add_argument('--service', '-s', required=True, help='Path to service account JSON')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--no-skip-existing', action='store_true', 
                       help='Process dates even if they already exist in BigQuery')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Parse dates
    start_date = None
    end_date = None
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    
    # Initialize configuration
    config = Configuration(
        project=args.project,
        service=args.service,
        verbose=args.verbose
    )
    
    # Run crawler
    crawler = DIBBSHistoricalCrawler(
        config=config,
        start_date=start_date,
        end_date=end_date,
        skip_existing=not args.no_skip_existing
    )
    
    crawler.run()
    
    return 0


if __name__ == '__main__':
    exit(main())