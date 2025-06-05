#!/usr/bin/env python3
"""
DIBBS Local Scraper - Downloads and saves files locally
Use this when Cloud services are unavailable
"""

import argparse
import csv
import io
import json
import logging
import os
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS_DOWNLOADS = 'https://www.dibbs.bsm.dla.mil/downloads/rfq/'
LOCAL_DATA_DIR = './dibbs_data'


class DIBBSLocalScraper:
    """Downloads DIBBS files and saves them locally"""
    
    def __init__(self, data_dir: str = LOCAL_DATA_DIR):
        self.data_dir = data_dir
        self.session = self._create_dibbs_session()
        self._ensure_directories()
    
    def _create_dibbs_session(self) -> requests.Session:
        """Create a session with DIBBS cookie authentication"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        
        try:
            # First, visit the warning page to get initial cookies
            logger.info("Initializing DIBBS session...")
            warning_url = f"{DIBBS_HOST}dodwarning.aspx?goto=/"
            response = session.get(warning_url, timeout=30)
            response.raise_for_status()
            
            # Parse the form to submit
            from html.parser import HTMLParser
            
            class FormParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.form_data = {}
                    self.form_action = ''
                    self.parsing_form = False
                
                def handle_starttag(self, tag, attrs):
                    if tag == 'form':
                        self.parsing_form = True
                        attrs_dict = dict(attrs)
                        self.form_action = attrs_dict.get('action', '')
                    elif self.parsing_form and tag == 'input':
                        attrs_dict = dict(attrs)
                        if 'name' in attrs_dict and 'value' in attrs_dict:
                            self.form_data[attrs_dict['name']] = attrs_dict['value']
                
                def handle_endtag(self, tag):
                    if tag == 'form':
                        self.parsing_form = False
            
            # Parse the form
            parser = FormParser()
            parser.feed(response.text)
            
            if parser.form_action and parser.form_data:
                # Submit the form to get authenticated
                form_url = DIBBS_HOST + parser.form_action.replace('./', '')
                logger.info(f"Submitting authentication form to {form_url}")
                form_response = session.post(form_url, data=parser.form_data, timeout=30)
                form_response.raise_for_status()
                logger.info("DIBBS session authenticated successfully")
            
        except Exception as e:
            logger.warning(f"Could not authenticate DIBBS session: {str(e)}")
            logger.warning("Proceeding without authentication - some downloads may fail")
        
        return session
    
    def _ensure_directories(self):
        """Create local directory structure"""
        dirs = [
            self.data_dir,
            os.path.join(self.data_dir, 'as_files'),
            os.path.join(self.data_dir, 'bq_files'),
            os.path.join(self.data_dir, 'in_files'),
            os.path.join(self.data_dir, 'as_extracted'),
            os.path.join(self.data_dir, 'bq_extracted'),
            os.path.join(self.data_dir, 'json_output')
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
    
    def download_file(self, url: str, local_path: str) -> bool:
        """Download a file from URL to local path"""
        try:
            logger.info(f"Downloading {url}")
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            logger.debug(f"Content-Type: {content_type}")
            
            # Check if we got HTML instead of the expected file
            if 'text/html' in content_type.lower() and not local_path.endswith('.html'):
                logger.warning(f"Got HTML instead of expected file type for {url}")
                # Check if it's the DoD warning page
                content_preview = response.content[:1000].decode('utf-8', errors='ignore')
                if 'Department of Defense' in content_preview and 'Warning and Consent' in content_preview:
                    logger.error(f"Got DoD warning page instead of file. Session may not be properly authenticated.")
                    return False
            
            # Save the file
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            # Verify downloaded files
            if local_path.endswith('.zip'):
                # Check if it's actually a ZIP file
                with open(local_path, 'rb') as f:
                    header = f.read(4)
                    if header[:2] != b'PK':
                        logger.error(f"Downloaded file is not a ZIP file: {local_path}")
                        os.remove(local_path)
                        return False
                
                # Try to open it
                try:
                    with zipfile.ZipFile(local_path, 'r') as zf:
                        # List contents
                        files = zf.namelist()
                        logger.info(f"ZIP contains {len(files)} files")
                except zipfile.BadZipFile:
                    logger.error(f"Invalid ZIP file: {local_path}")
                    os.remove(local_path)
                    return False
            
            elif local_path.endswith('.txt'):
                # Check if it's actually a text file (not HTML)
                with open(local_path, 'rb') as f:
                    content = f.read(500)
                    if b'<!DOCTYPE html' in content or b'<html' in content:
                        logger.error(f"Downloaded .txt file is actually HTML: {local_path}")
                        os.remove(local_path)
                        return False
            
            logger.info(f"Successfully downloaded to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False
    
    def process_as_file(self, date_str: str) -> List[Dict]:
        """Process approved sources (as) file"""
        filename = f"as{date_str}.zip"
        url = f"{DIBBS_DOWNLOADS}{filename}"
        local_zip = os.path.join(self.data_dir, 'as_files', filename)
        
        # Check if already downloaded and valid
        if os.path.exists(local_zip):
            # Check if it's a valid ZIP
            try:
                with zipfile.ZipFile(local_zip, 'r') as zf:
                    # If we can open it, it's valid
                    logger.info(f"Using existing valid ZIP file: {local_zip}")
            except:
                logger.warning(f"Existing file is not a valid ZIP, re-downloading: {local_zip}")
                os.remove(local_zip)
                if not self.download_file(url, local_zip):
                    return []
        else:
            if not self.download_file(url, local_zip):
                return []
        
        approved_sources = []
        
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                # Extract to local directory
                extract_dir = os.path.join(self.data_dir, 'as_extracted', date_str)
                zf.extractall(extract_dir)
                
                # Find the .txt file inside
                txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
                if txt_files:
                    txt_path = os.path.join(extract_dir, txt_files[0])
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        
                        for row in reader:
                            if len(row) >= 3:
                                # Clean NSN by removing hyphens
                                nsn = row[0].strip('"').replace('-', '')
                                approved_sources.append({
                                    'nsn': nsn,
                                    'supplier_cage_code': row[1].strip('"'),
                                    'supplier_part_number': row[2].strip('"'),
                                    'supplier_name': row[3].strip('"') if len(row) > 3 else None,
                                    'file_date': datetime.strptime(date_str, '%y%m%d').strftime('%Y-%m-%d'),
                                    'source_file': filename
                                })
            
            logger.info(f"Processed {len(approved_sources)} approved sources from {filename}")
            
            # Save to JSON
            json_file = os.path.join(self.data_dir, 'json_output', f'as_{date_str}.json')
            with open(json_file, 'w') as f:
                json.dump(approved_sources, f, indent=2)
            logger.info(f"Saved AS data to {json_file}")
            
        except zipfile.BadZipFile:
            logger.error(f"Error: {filename} is not a valid ZIP file")
            # Check what we actually downloaded
            with open(local_zip, 'rb') as f:
                content = f.read(500).decode('utf-8', errors='ignore')
                logger.error(f"File content preview: {content[:200]}...")
        except Exception as e:
            logger.error(f"Error processing AS file {filename}: {str(e)}")
        
        return approved_sources
    
    def process_bq_file(self, date_str: str) -> List[Dict]:
        """Process batch quote (bq) file"""
        filename = f"bq{date_str}.zip"
        url = f"{DIBBS_DOWNLOADS}{filename}"
        local_zip = os.path.join(self.data_dir, 'bq_files', filename)
        
        # Check if already downloaded and valid
        if os.path.exists(local_zip):
            # Check if it's a valid ZIP
            try:
                with zipfile.ZipFile(local_zip, 'r') as zf:
                    # If we can open it, it's valid
                    logger.info(f"Using existing valid ZIP file: {local_zip}")
            except:
                logger.warning(f"Existing file is not a valid ZIP, re-downloading: {local_zip}")
                os.remove(local_zip)
                if not self.download_file(url, local_zip):
                    return []
        else:
            if not self.download_file(url, local_zip):
                return []
        
        batch_quotes = []
        
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                # Extract to local directory
                extract_dir = os.path.join(self.data_dir, 'bq_extracted', date_str)
                zf.extractall(extract_dir)
                
                # Find the .txt file inside
                txt_files = [f for f in zf.namelist() if f.endswith('.txt') and 'bq' in f.lower()]
                if txt_files:
                    txt_path = os.path.join(extract_dir, txt_files[0])
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        
                        for row_num, row in enumerate(reader):
                            if len(row) >= 46:  # Minimum required columns
                                try:
                                    batch_quotes.append({
                                        'solicitation_number': row[0].strip('"').replace('-', ''),
                                        'solicitation_type': row[1].strip('"'),
                                        'small_business_setaside': row[2].strip('"'),
                                        'additional_clause_fillins': row[3].strip('"') == 'Y',
                                        'return_by_date': self._parse_date(row[4].strip('"')),
                                        'bid_type': row[23].strip('"') if len(row) > 23 else None,
                                        'discount_terms': row[24].strip('"') if len(row) > 24 else None,
                                        'days_quote_valid': int(row[26].strip('"')) if len(row) > 26 and row[26].strip('"').isdigit() else None,
                                        'fob_point': row[31].strip('"') if len(row) > 31 else None,
                                        'inspection_point': row[35].strip('"') if len(row) > 35 else None,
                                        'solicitation_line': row[43].strip('"') if len(row) > 43 else None,
                                        'purchase_request': row[45].strip('"') if len(row) > 45 else None,
                                        'nsn_part_number': row[46].strip('"').replace('-', '') if len(row) > 46 else None,
                                        'unit_of_issue': row[47].strip('"') if len(row) > 47 else None,
                                        'quantity': int(row[48].strip('"')) if len(row) > 48 and row[48].strip('"').isdigit() else None,
                                        'unit_price': float(row[49].strip('"')) if len(row) > 49 and row[49].strip('"').replace('.', '').isdigit() else None,
                                        'delivery_days': int(row[50].strip('"')) if len(row) > 50 and row[50].strip('"').isdigit() else None,
                                        'file_date': datetime.strptime(date_str, '%y%m%d').strftime('%Y-%m-%d'),
                                        'source_file': filename
                                    })
                                except Exception as e:
                                    logger.warning(f"Error processing BQ row {row_num}: {str(e)}")
            
            logger.info(f"Processed {len(batch_quotes)} batch quotes from {filename}")
            
            # Save to JSON
            json_file = os.path.join(self.data_dir, 'json_output', f'bq_{date_str}.json')
            with open(json_file, 'w') as f:
                json.dump(batch_quotes, f, indent=2)
            logger.info(f"Saved BQ data to {json_file}")
            
        except zipfile.BadZipFile:
            logger.error(f"Error: {filename} is not a valid ZIP file")
            # Check what we actually downloaded
            with open(local_zip, 'rb') as f:
                content = f.read(500).decode('utf-8', errors='ignore')
                logger.error(f"File content preview: {content[:200]}...")
        except Exception as e:
            logger.error(f"Error processing BQ file {filename}: {str(e)}")
        
        return batch_quotes
    
    def process_in_file(self, date_str: str) -> List[Dict]:
        """Process index (in) file"""
        filename = f"in{date_str}.txt"
        url = f"{DIBBS_DOWNLOADS}{filename}"
        local_file = os.path.join(self.data_dir, 'in_files', filename)
        
        # Check if already downloaded
        if os.path.exists(local_file):
            logger.info(f"Using existing file: {local_file}")
        else:
            if not self.download_file(url, local_file):
                return []
        
        index_data = []
        
        try:
            with open(local_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if len(line) >= 137:  # Minimum line length
                        # Parse fixed-width format
                        solicitation = line[0:13].strip()
                        nsn_part = line[13:59].strip().replace('-', '')
                        purchase_request = line[59:72].strip()
                        return_by = line[72:80].strip()
                        file_name = line[80:99].strip()
                        qty = line[99:106].strip()
                        unit_issue = line[106:108].strip()
                        nomenclature = line[108:129].strip()
                        buyer_code = line[129:134].strip()
                        amsc = line[134:135].strip()
                        item_type = line[135:136].strip() if len(line) > 135 else ''
                        setaside = line[136:137].strip() if len(line) > 136 else ''
                        setaside_pct = line[137:140].strip() if len(line) > 139 else ''
                        
                        index_data.append({
                            'solicitation_number': solicitation.replace('-', ''),
                            'nsn_part_number': nsn_part,
                            'purchase_request': purchase_request,
                            'return_by_date': self._parse_date(return_by),
                            'pdf_filename': file_name,
                            'quantity': int(qty) if qty.isdigit() else None,
                            'unit_of_issue': unit_issue,
                            'nomenclature': nomenclature,
                            'buyer_code': buyer_code,
                            'amsc': amsc,
                            'item_type_indicator': item_type,
                            'small_business_setaside': setaside,
                            'setaside_percentage': int(setaside_pct) if setaside_pct.isdigit() else None,
                            'file_date': datetime.strptime(date_str, '%y%m%d').strftime('%Y-%m-%d'),
                            'source_file': filename
                        })
            
            logger.info(f"Processed {len(index_data)} index entries from {filename}")
            
            # Save to JSON
            json_file = os.path.join(self.data_dir, 'json_output', f'in_{date_str}.json')
            with open(json_file, 'w') as f:
                json.dump(index_data, f, indent=2)
            logger.info(f"Saved IN data to {json_file}")
            
        except Exception as e:
            logger.error(f"Error processing IN file {filename}: {str(e)}")
        
        return index_data
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date from MM/DD/YYYY or MM-DD-YYYY to YYYY-MM-DD"""
        if not date_str:
            return None
        
        try:
            # Try different date formats
            for fmt in ['%m/%d/%Y', '%m-%d-%Y', '%m%d%Y', '%m%d%y']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def generate_summary_report(self, start_date: datetime, end_date: datetime):
        """Generate a summary report of all processed data"""
        report = {
            'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            'summary': {
                'as_files': 0,
                'bq_files': 0,
                'in_files': 0,
                'total_approved_sources': 0,
                'total_batch_quotes': 0,
                'total_index_entries': 0
            },
            'files_processed': []
        }
        
        # Count files and records
        json_dir = os.path.join(self.data_dir, 'json_output')
        for filename in os.listdir(json_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(json_dir, filename)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                if filename.startswith('as_'):
                    report['summary']['as_files'] += 1
                    report['summary']['total_approved_sources'] += len(data)
                elif filename.startswith('bq_'):
                    report['summary']['bq_files'] += 1
                    report['summary']['total_batch_quotes'] += len(data)
                elif filename.startswith('in_'):
                    report['summary']['in_files'] += 1
                    report['summary']['total_index_entries'] += len(data)
                
                report['files_processed'].append({
                    'filename': filename,
                    'records': len(data)
                })
        
        # Save report
        report_file = os.path.join(self.data_dir, 'processing_report.json')
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Summary report saved to {report_file}")
        return report


def process_date_range(scraper: DIBBSLocalScraper, start_date: datetime, 
                      end_date: datetime, file_types: List[str] = ['as', 'bq', 'in']) -> Dict[str, int]:
    """Process files for a date range"""
    stats = {'as': 0, 'bq': 0, 'in': 0, 'errors': 0}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%y%m%d')
        logger.info(f"\nProcessing date: {date_str}")
        
        # Process AS files
        if 'as' in file_types:
            try:
                as_data = scraper.process_as_file(date_str)
                stats['as'] += len(as_data)
            except Exception as e:
                logger.error(f"Error processing AS file for {date_str}: {str(e)}")
                stats['errors'] += 1
        
        # Process BQ files
        if 'bq' in file_types:
            try:
                bq_data = scraper.process_bq_file(date_str)
                stats['bq'] += len(bq_data)
            except Exception as e:
                logger.error(f"Error processing BQ file for {date_str}: {str(e)}")
                stats['errors'] += 1
        
        # Process IN files
        if 'in' in file_types:
            try:
                in_data = scraper.process_in_file(date_str)
                stats['in'] += len(in_data)
            except Exception as e:
                logger.error(f"Error processing IN file for {date_str}: {str(e)}")
                stats['errors'] += 1
        
        current_date += timedelta(days=1)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='DIBBS Local Data Scraper')
    parser.add_argument('--start-date', '-sd', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', '-ed', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--data-dir', '-d', default=LOCAL_DATA_DIR, 
                       help='Local directory for data storage')
    parser.add_argument('--file-types', '-ft', nargs='+', default=['as', 'bq', 'in'],
                       choices=['as', 'bq', 'in'], help='File types to process')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        logger.info(f"Starting DIBBS local scraper")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        logger.info(f"Data directory: {args.data_dir}")
        logger.info(f"File types: {args.file_types}")
        
        # Initialize scraper
        scraper = DIBBSLocalScraper(args.data_dir)
        
        # Process files
        stats = process_date_range(scraper, start_date, end_date, args.file_types)
        
        # Generate summary report
        report = scraper.generate_summary_report(start_date, end_date)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"Processing Complete!")
        print(f"{'='*60}")
        print(f"AS Records: {stats['as']:,}")
        print(f"BQ Records: {stats['bq']:,}")
        print(f"IN Records: {stats['in']:,}")
        print(f"Errors: {stats['errors']}")
        print(f"\nData saved to: {args.data_dir}")
        print(f"Check {os.path.join(args.data_dir, 'processing_report.json')} for details")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())