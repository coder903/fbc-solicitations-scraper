#!/usr/bin/env python3
"""
DIBBS RFQ File Downloader - Downloads ca, bq, as, and in files using proven authentication
"""

import argparse
import logging
import os
import requests
import time
import warnings
import zipfile
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import Dict, List, Optional
import ssl

# Suppress SSL warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings('ignore', category=InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'

# Headers from dibbs_awards_scraper.py
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Host': 'www.dibbs.bsm.dla.mil',
    'Pragma': 'no-cache',
    'Referer': 'https://www.dibbs.bsm.dla.mil/dodwarning.aspx?goto=/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}


class FormParser(HTMLParser):
    """Reusing FormParser from dibbs_awards_scraper.py"""
    @staticmethod
    def parse(page):
        parser = FormParser()
        parser.feed(page)
        parser.close()
        form_data, form_action, form_method = parser.get_data()
        form_action = form_action.replace('./', '')
        return form_data, form_action, form_method

    def __init__(self):
        super().__init__()
        self.form_data = {}
        self.form_action = ''
        self.form_method = 'get'
        self.parsing_form = False

    def handle_starttag(self, tag, attrs):
        if tag == 'form':
            self.parsing_form = True
            attrs = dict(attrs)
            self.form_action = attrs['action'] 
            self.form_method = attrs['method'].upper() 
            self.form_data = {}
        elif self.parsing_form and tag == 'input':
            attrs = dict(attrs)
            if 'name' in attrs and 'value' in attrs:
                self.form_data[attrs['name']] = attrs['value']

    def handle_endtag(self, tag):
        if tag == 'form':
            self.parsing_form = False

    def get_data(self):
        return self.form_data, self.form_action, self.form_method


def dibbs_session(host, verify=True, max_retries=3):
    """
    Reusing the session creation from dibbs_awards_scraper.py
    """
    session = requests.Session()
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempt {attempt+1} to establish DIBBS session with {host}")
            
            # Load first page
            page = session.request('GET', url=host + "dodwarning.aspx?goto=/", verify=verify).content.decode('utf-8')
            
            # Parse form
            form_data, form_action, form_method = FormParser().parse(page)
            
            # Submit form to set cookie
            page = session.request('POST', 
                url=host + form_action,
                data=form_data,
                verify=verify
            )
            
            logger.info(f"Successfully established DIBBS session with {host}")
            return session
            
        except Exception as e:
            logger.warning(f"Session creation failed on attempt {attempt+1}: {str(e)}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)


class DIBBSDownloader:
    def __init__(self):
        # Create sessions for both DIBBS and DIBBS2
        self.session_dibbs = dibbs_session(DIBBS_HOST)
        self.session_dibbs2 = None
        
    def get_dibbs2_session(self):
        """Get or create DIBBS2 session"""
        if not self.session_dibbs2:
            self.session_dibbs2 = dibbs_session(DIBBS2_HOST, verify=False)
        return self.session_dibbs2
    
    def download_file(self, url: str, output_path: str, max_retries: int = 3) -> bool:
        """Download a file with retry logic"""
        # Determine which session to use based on URL
        if 'dibbs2' in url:
            session = self.get_dibbs2_session()
            verify = False
            headers = {
                'Host': 'dibbs2.bsm.dla.mil',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
            }
        else:
            session = self.session_dibbs
            verify = True
            headers = {}
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {os.path.basename(url)} (attempt {attempt + 1})")
                
                response = session.get(url, timeout=60, stream=True, verify=verify, headers=headers)
                response.raise_for_status()
                
                # Check if we got HTML instead of the expected file
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type and not output_path.endswith('.html'):
                    # Check if it's the DoD warning page
                    content_preview = response.content[:1000].decode('utf-8', errors='ignore')
                    if 'dodwarning' in content_preview.lower():
                        logger.warning("Got DoD warning page, re-authenticating...")
                        # Re-create session
                        if 'dibbs2' in url:
                            self.session_dibbs2 = dibbs_session(DIBBS2_HOST, verify=False)
                        else:
                            self.session_dibbs = dibbs_session(DIBBS_HOST)
                        continue
                
                # Save file
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Verify file
                file_size = os.path.getsize(output_path)
                logger.info(f"Downloaded {file_size:,} bytes")
                
                # Verify ZIP files
                if output_path.endswith('.zip'):
                    try:
                        with zipfile.ZipFile(output_path, 'r') as zf:
                            files = zf.namelist()
                            logger.info(f"ZIP verified, contains {len(files)} files")
                            return True
                    except zipfile.BadZipFile:
                        logger.error("Invalid ZIP file, removing")
                        os.remove(output_path)
                        if attempt < max_retries - 1:
                            time.sleep(2 ** attempt)
                            continue
                        return False
                
                # Verify text files aren't HTML
                elif output_path.endswith('.txt'):
                    with open(output_path, 'rb') as f:
                        content = f.read(100)
                        if b'<html' in content.lower() or b'<!doctype' in content.lower():
                            logger.error("Text file is actually HTML, removing")
                            os.remove(output_path)
                            if attempt < max_retries - 1:
                                time.sleep(2 ** attempt)
                                continue
                            return False
                
                return True
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Download error: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
        return False
    
    def download_files_for_date(self, date: datetime, output_dir: str, 
                               file_types: List[str]) -> Dict[str, int]:
        """Download all files for a specific date"""
        date_code = date.strftime('%y%m%d')
        stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}
        
        logger.info(f"\nProcessing {date.strftime('%Y-%m-%d')} (code: {date_code})")
        
        # Define files to download
        files = []
        
        if 'ca' in file_types:
            files.append({
                'type': 'complete_archive',
                'filename': f'ca{date_code}.zip',
                'url': f'{DIBBS2_HOST}Downloads/RFQ/Archive/ca{date_code}.zip',
                'subdir': 'ca_files'
            })
        
        
        if 'bq' in file_types:
            files.append({
                'type': 'batch_quote',
                'filename': f'bq{date_code}.zip',
                'url': f'{DIBBS2_HOST}Downloads/RFQ/Archive/bq{date_code}.zip',
                'subdir': 'bq_files'
            })
        
        # Note: AS files are typically included inside BQ zip files
        # Only try to download separately if specifically requested
        if 'as' in file_types and 'bq' not in file_types:
            files.append({
                'type': 'approved_sources',
                'filename': f'as{date_code}.zip',
                'url': f'{DIBBS2_HOST}Downloads/RFQ/Archive/as{date_code}.zip',
                'subdir': 'as_files'
            })
        
        if 'in' in file_types:
            files.append({
                'type': 'index',
                'filename': f'in{date_code}.txt',
                'url': f'{DIBBS2_HOST}Downloads/RFQ/Archive/in{date_code}.txt',
                'subdir': 'in_files'
            })
        
        # Add delay between downloads
        delay_between_downloads = 1
        
        # Download each file
        for i, file_info in enumerate(files):
            if i > 0:
                time.sleep(delay_between_downloads)
                
            output_path = os.path.join(output_dir, file_info['subdir'], file_info['filename'])
            
            # Check if already exists and is valid
            if os.path.exists(output_path):
                # Verify existing file
                is_valid = True
                
                if output_path.endswith('.zip'):
                    try:
                        with zipfile.ZipFile(output_path, 'r') as zf:
                            pass
                    except:
                        is_valid = False
                        logger.warning(f"Existing file {file_info['filename']} is invalid, re-downloading")
                
                elif output_path.endswith('.txt'):
                    with open(output_path, 'rb') as f:
                        content = f.read(100)
                        if b'<html' in content.lower():
                            is_valid = False
                            logger.warning(f"Existing file {file_info['filename']} is HTML, re-downloading")
                
                if is_valid:
                    logger.info(f"Skipping {file_info['filename']} - valid file exists")
                    stats['skipped'] += 1
                    continue
                else:
                    os.remove(output_path)
            
            # Try to download
            if self.download_file(file_info['url'], output_path):
                stats['downloaded'] += 1
            else:
                stats['failed'] += 1
        
        return stats


def main():
    parser = argparse.ArgumentParser(description='DIBBS RFQ File Downloader')
    parser.add_argument('--start-date', '-s', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', '-e', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', '-o', default='./dibbs_data', help='Output directory')
    parser.add_argument('--file-types', '-f', nargs='+', 
                       default=['in', 'bq'],
                       choices=['ca', 'in', 'bq', 'as'],
                       help='File types to download (default: in, bq)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD")
        return 1
    
    # Initialize downloader
    downloader = DIBBSDownloader()
    
    # Process files
    total_stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}
    
    current_date = start_date
    while current_date <= end_date:
        stats = downloader.download_files_for_date(current_date, args.output_dir, args.file_types)
        
        total_stats['downloaded'] += stats['downloaded']
        total_stats['skipped'] += stats['skipped']
        total_stats['failed'] += stats['failed']
        
        current_date += timedelta(days=1)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Download Summary")
    print(f"{'='*60}")
    print(f"Downloaded: {total_stats['downloaded']} files")
    print(f"Skipped: {total_stats['skipped']} files (valid files exist)")
    print(f"Failed: {total_stats['failed']} files")
    print(f"\nData saved to: {args.output_dir}")
    
    # Note about AS files
    if 'bq' in args.file_types:
        print("\nNote: AS (Approved Sources) files are included within BQ zip files.")
        print("The file processor will extract them automatically.")
    
    return 0


if __name__ == '__main__':
    exit(main())