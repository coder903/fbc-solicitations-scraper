#!/usr/bin/env python3
"""
DIBBS Historical Data Scraper
Processes historical solicitation data using bqothers.zip and limited web scraping
"""

import argparse
import json
import logging
import os
import re
import time
import zipfile
import io
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import requests
import urllib3

from util.storage_api import Storage
from util.configuration import Configuration
from util.bigquery_api import BigQuery

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import parser and session functions
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from solicitations import dibbs_session, RfqRecsParser, FormParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
RFQ_PROJECT = 'argon-surf-426917-k8'
DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'
BUCKET = 'fbc-requests'
DATASET = 'DIBBS'
BQ_DATASET = 'REQUESTS'
BQOTHERS_URL = 'https://dibbs2.bsm.dla.mil/Downloads/RFQ/Archive/bqothers.zip'

# Use exact same schema as combined.py for compatibility
SOLICITATIONS_SCHEMA = [
    {"name": "solicitation_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "solicitation_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "issued_date", "type": "DATE", "mode": "NULLABLE"},  # From web scrape
    {"name": "return_by_date", "type": "DATETIME", "mode": "NULLABLE"},
    {"name": "posted_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "last_updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
    
    {"name": "solicitation_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "small_business_setaside", "type": "STRING", "mode": "NULLABLE"},
    {"name": "setaside_percentage", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "additional_clause_fillins", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "discount_terms", "type": "STRING", "mode": "NULLABLE"},
    {"name": "days_quote_valid", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "fob_point", "type": "STRING", "mode": "NULLABLE"},
    {"name": "inspection_point", "type": "STRING", "mode": "NULLABLE"},
    {"name": "amsc", "type": "STRING", "mode": "NULLABLE"},  # Not available for historical
    
    {"name": "guaranteed_minimum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "do_minimum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "contract_maximum", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "annual_frequency_buys", "type": "INTEGER", "mode": "NULLABLE"},
    
    {
        "name": "clins",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"name": "clin", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nsn", "type": "STRING", "mode": "NULLABLE"},
            {"name": "part_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "nomenclature", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pr_number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "unit_of_issue", "type": "STRING", "mode": "NULLABLE"},
            {"name": "unit_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "delivery_days", "type": "INTEGER", "mode": "NULLABLE"},
            
            {
                "name": "technical_documents",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "url", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
            
            {
                "name": "approved_sources",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "supplier_cage_code", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "supplier_part_number", "type": "STRING", "mode": "NULLABLE"},
                ]
            }
        ]
    },
    
    {"name": "setaside_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "rfq_quote_status", "type": "STRING", "mode": "NULLABLE"},
    
    {
        "name": "links",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "solicitation_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "package_view_url", "type": "STRING", "mode": "NULLABLE"},
        ]
    },
    
    {"name": "scrape_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "data_source", "type": "STRING", "mode": "NULLABLE"},
    {"name": "raw_batch_quote_data", "type": "JSON", "mode": "NULLABLE"},
]


class HistoricalDataProcessor:
    def __init__(self, config, test_mode=False, dry_run=False):
        self.config = config
        self.storage = Storage(config, "service")
        self.bq = BigQuery(config, "service")
        self.test_mode = test_mode
        self.dry_run = dry_run
        
        # Sessions
        self.session_dibbs = dibbs_session(DIBBS_HOST)
        self.session_dibbs2 = dibbs_session(DIBBS2_HOST, verify=False)
        
        # Historical data cache
        self.historical_data = {}  # date -> solicitation -> records
        self.historical_loaded = False
        
        if self.dry_run:
            logger.info("DRY RUN MODE - No data will be written to GCS or BigQuery")
        
    def download_bqothers(self) -> Optional[bytes]:
        """Download the bqothers.zip file"""
        logger.info("Downloading bqothers.zip...")
        try:
            response = self.session_dibbs2.get(BQOTHERS_URL, timeout=60, verify=False)
            response.raise_for_status()
            logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.2f}MB")
            return response.content
        except Exception as e:
            logger.error(f"Error downloading bqothers.zip: {str(e)}")
            return None
    
    def parse_bq_line(self, line: str) -> Optional[Dict]:
        """Parse a single line from batch quote file"""
        line = line.strip()
        if not line:
            return None
            
        # Parse CSV with quotes
        parts = []
        current = ''
        in_quotes = False
        
        for char in line:
            if char == '"' and not in_quotes:
                in_quotes = True
            elif char == '"' and in_quotes:
                in_quotes = False
            elif char == ',' and not in_quotes:
                parts.append(current)
                current = ''
            else:
                current += char
        
        if current:
            parts.append(current)
        
        # Need at least 51 fields for basic data
        if len(parts) < 51:
            return None
            
        try:
            record = {
                'solicitation_number': parts[0].strip('"'),
                'solicitation_type': parts[1].strip('"'),
                'small_business_setaside': parts[2].strip('"'),
                'additional_clause_fillins': parts[3].strip('"') == 'Y',
                'return_by_date': parts[4].strip('"'),
                'bid_type': parts[23].strip('"') if len(parts) > 23 else '',
                'discount_terms': parts[24].strip('"') if len(parts) > 24 else '',
                'days_quote_valid': self._parse_int(parts[26]) if len(parts) > 26 else None,
                'fob_point': parts[31].strip('"') if len(parts) > 31 else '',
                'inspection_point': parts[35].strip('"') if len(parts) > 35 else '',
                'line_number': parts[43].strip('"') if len(parts) > 43 else '',
                'purchase_request': parts[45].strip('"') if len(parts) > 45 else '',
                'nsn_part_number': parts[46].strip('"') if len(parts) > 46 else '',
                'unit_of_issue': parts[47].strip('"') if len(parts) > 47 else '',
                'quantity': self._parse_int(parts[48]) if len(parts) > 48 else None,
                'unit_price': self._parse_float(parts[49]) if len(parts) > 49 else None,
                'delivery_days': self._parse_int(parts[50]) if len(parts) > 50 else None,
            }
            
            # Extract AIDC contract terms if present
            if len(parts) > 54:
                record['guaranteed_minimum'] = self._parse_int(parts[51])
                record['do_minimum'] = self._parse_int(parts[52])
                record['contract_maximum'] = self._parse_int(parts[53])
                record['annual_frequency_buys'] = self._parse_int(parts[54])
                
            return record
            
        except Exception as e:
            logger.debug(f"Error parsing line: {str(e)}")
            return None
    
    def load_historical_data(self):
        """Load and index all historical data from bqothers.zip"""
        if self.historical_loaded:
            return
            
        logger.info("Loading historical data from bqothers.zip...")
        
        # Download the file
        content = self.download_bqothers()
        if not content:
            logger.error("Failed to download bqothers.zip")
            return
            
        # Extract and parse
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Find the bqothers.txt file
                bq_filename = None
                for name in zf.namelist():
                    if name.lower().startswith('bqothers') and name.lower().endswith('.txt'):
                        bq_filename = name
                        break
                
                if not bq_filename:
                    logger.error("No bqothers.txt file found in archive")
                    return
                    
                # Read and parse the file
                logger.info(f"Parsing {bq_filename}...")
                bq_text = zf.read(bq_filename).decode('utf-8', errors='ignore')
                
                line_count = 0
                record_count = 0
                
                for line in bq_text.split('\n'):
                    line_count += 1
                    if line_count % 1000 == 0:
                        logger.info(f"  Processed {line_count} lines, {record_count} records...")
                        
                    record = self.parse_bq_line(line)
                    if not record:
                        continue
                        
                    # Parse return by date to get the date key
                    date_str = self.parse_date(record['return_by_date'])
                    if not date_str:
                        continue
                        
                    # Index by date and solicitation
                    if date_str not in self.historical_data:
                        self.historical_data[date_str] = defaultdict(list)
                        
                    sol_num = record['solicitation_number']
                    self.historical_data[date_str][sol_num].append(record)
                    record_count += 1
                
                logger.info(f"Loaded {record_count} records covering {len(self.historical_data)} dates")
                
                # Show date range
                if self.historical_data:
                    dates = sorted(self.historical_data.keys())
                    logger.info(f"Date range: {dates[0]} to {dates[-1]}")
                    
                self.historical_loaded = True
                
        except Exception as e:
            logger.error(f"Error processing bqothers.zip: {str(e)}")
    
    def _parse_int(self, value) -> Optional[int]:
        """Safely parse integer value"""
        if value is None or value == '':
            return None
        try:
            cleaned = str(value).strip().replace(',', '')
            return int(cleaned)
        except:
            return None
    
    def _parse_float(self, value) -> Optional[float]:
        """Safely parse float value"""
        if value is None or value == '':
            return None
        try:
            cleaned = str(value).strip().replace(',', '')
            return float(cleaned)
        except:
            return None
    
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to YYYY-MM-DD"""
        if not date_str or date_str.strip() == '':
            return None
            
        formats = ['%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%m/%d/%y', '%m%d%Y', '%m%d%y']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime('%Y-%m-%d')
            except:
                continue
        return None
    
    def scrape_historical_web_data(self, date_str: str) -> List[Dict]:
        """Attempt to scrape historical data from web"""
        # Convert to web format (MM-DD-YYYY)
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            web_date_str = date_obj.strftime('%m-%d-%Y')
        except:
            return []
            
        logger.info(f"Attempting web scrape for {web_date_str}...")
        
        try:
            # Use the same scraping logic as combined.py
            number = 1
            rows = []

            logger.info(f'  PAGE {number}')

            # Load first page
            form_action = f'{DIBBS_HOST}RFQ/RfqRecs.aspx?category=post&TypeSrch=dt&Value={web_date_str}'
            page = self.session_dibbs.request('GET', form_action).content.decode('utf-8')
            
            form_data, _, form_method = FormParser().parse(page)
            count, rows_more = RfqRecsParser().parse(page)
            rows.extend(rows_more)

            logger.info(f"  Found {count} total records. Retrieved {len(rows_more)} from page {number}")

            # Limit for test mode
            max_records = 30 if self.test_mode else None
            
            # Check if we've reached the test limit
            if self.test_mode and max_records and len(rows) >= max_records:
                logger.info(f'  TEST MODE: Stopping at {len(rows)} records')
                return rows[:max_records]

            # Loop additional pages
            while len(rows) < count and len(rows_more) > 0:
                # Check test limit before fetching next page
                if self.test_mode and max_records and len(rows) >= max_records:
                    logger.info(f'  TEST MODE: Stopping at {len(rows)} records')
                    return rows[:max_records]
                    
                number += 1
                logger.info(f'  PAGE {number} ({len(rows)} of {count})')

                form_data['__EVENTTARGET'] = 'ctl00$cph1$grdRfqSearch'
                form_data['__EVENTARGUMENT'] = f'Page${number}'

                # Default select fields
                form_data['ctl00$ddlNavigation'] = '' 
                form_data['ctl00$ddlGotoDB'] = '' 
                form_data['ctl00$txtValue'] = ''

                # Remove buttons if they exist
                if "ctl00$butNavigation" in form_data:
                    del form_data["ctl00$butNavigation"]
                if "ctl00$butDbSearch" in form_data:
                    del form_data["ctl00$butDbSearch"]

                page = self.session_dibbs.request('POST',
                    url=form_action,
                    data=form_data,
                ).content.decode('utf-8')

                form_data, _, form_method = FormParser().parse(page)
                _, rows_more = RfqRecsParser().parse(page)
                rows.extend(rows_more)
                
                logger.info(f"  Retrieved {len(rows_more)} records from page {number}")
            
            logger.info(f"  Found {len(rows)} records via web scrape")
            return rows
            
        except Exception as e:
            logger.warning(f"  Web scrape failed: {str(e)}")
            return []
    
    def merge_historical_data(self, date_str: str, web_data: List[Dict], 
                            batch_data: Dict[str, List[Dict]]) -> List[Dict]:
        """Merge web scrape and batch data for a date"""
        solicitations = {}
        
        # Process batch data first (more complete)
        for sol_num, records in batch_data.items():
            if sol_num not in solicitations:
                solicitations[sol_num] = {
                    'solicitation_number': sol_num,
                    'solicitation_date': date_str,
                    'scrape_date': date_str,  # Date this data is for
                    'clins': [],
                    'data_source': 'bqothers',
                    'data_completeness': 'batch_only',
                    'scrape_timestamp': datetime.utcnow().isoformat() + 'Z',
                    'last_updated': datetime.utcnow().isoformat() + 'Z'
                }
            
            sol = solicitations[sol_num]
            
            # Use first record for header data
            if records:
                first = records[0]
                sol.update({
                    'solicitation_type': first.get('solicitation_type'),
                    'small_business_setaside': first.get('small_business_setaside'),
                    'additional_clause_fillins': first.get('additional_clause_fillins'),
                    'return_by_date': self.parse_date(first.get('return_by_date')),
                    'bid_type': first.get('bid_type'),
                    'fob_point': first.get('fob_point'),
                    'inspection_point': first.get('inspection_point'),
                    'discount_terms': first.get('discount_terms'),
                    'days_quote_valid': first.get('days_quote_valid'),
                    'guaranteed_minimum': first.get('guaranteed_minimum'),
                    'do_minimum': first.get('do_minimum'),
                    'contract_maximum': first.get('contract_maximum'),
                    'annual_frequency_buys': first.get('annual_frequency_buys'),
                })
            
            # Process CLINs
            for record in records:
                clin = {
                    'clin': record.get('line_number', '').zfill(4) if record.get('line_number') else None,
                    'nsn': record.get('nsn_part_number', '').replace('-', ''),
                    'part_number': record.get('nsn_part_number', ''),
                    'nomenclature': None,  # Not in batch data
                    'pr_number': record.get('purchase_request'),
                    'quantity': record.get('quantity'),
                    'unit_of_issue': record.get('unit_of_issue'),
                    'unit_price': record.get('unit_price'),
                    'delivery_days': record.get('delivery_days'),
                    'technical_documents': [],  # Empty array for compatibility
                    'approved_sources': []  # Empty array for compatibility
                }
                sol['clins'].append(clin)
        
        # Enhance with web data if available
        for record in web_data:
            sol_num = record.get('solicitation', '').strip()
            if not sol_num:
                continue
                
            if sol_num in solicitations:
                # Update existing record
                sol = solicitations[sol_num]
                sol['data_source'] = 'historical_both'
                
                # Add web-specific fields including issued_date
                sol.update({
                    'setaside_type': record.get('setaside_type'),
                    'rfq_quote_status': record.get('rfq_quote_status'),
                    'issued_date': self.parse_date(record.get('issued', '')),  # Parse issued date
                    'links': {
                        'solicitation_url': record.get('solicitation_url'),
                        'package_view_url': None  # Not available in historical
                    }
                })
                
                # Try to match and enhance CLINs with nomenclature
                nsn_clean = record.get('nsn_part_number', '').replace('-', '')
                for clin in sol['clins']:
                    if clin['nsn'] == nsn_clean:
                        clin['nomenclature'] = record.get('nomenclature')
                        break
            else:
                # Create new record from web data only
                solicitations[sol_num] = {
                    'solicitation_number': sol_num,
                    'solicitation_date': date_str,
                    'issued_date': self.parse_date(record.get('issued', '')),  # Parse issued date
                    'return_by_date': self.parse_date(record.get('return_by', '')),  # Parse return by date
                    'posted_date': None,
                    'last_updated': datetime.utcnow().isoformat() + 'Z',
                    'solicitation_type': None,
                    'small_business_setaside': None,
                    'setaside_percentage': None,
                    'additional_clause_fillins': None,
                    'discount_terms': None,
                    'days_quote_valid': None,
                    'fob_point': None,
                    'inspection_point': None,
                    'amsc': None,
                    'guaranteed_minimum': None,
                    'do_minimum': None,
                    'contract_maximum': None,
                    'annual_frequency_buys': None,
                    'clins': [{
                        'clin': None,
                        'nsn': record.get('nsn_part_number', '').replace('-', ''),
                        'part_number': record.get('nsn_part_number', ''),
                        'nomenclature': record.get('nomenclature'),
                        'pr_number': record.get('pr_number'),
                        'quantity': self._parse_int(record.get('quantity')),
                        'unit_of_issue': None,
                        'unit_price': None,
                        'delivery_days': None,
                        'technical_documents': [],  # Empty array
                        'approved_sources': []  # Empty array
                    }],
                    'setaside_type': record.get('setaside_type'),
                    'rfq_quote_status': record.get('rfq_quote_status'),
                    'links': {
                        'solicitation_url': record.get('solicitation_url'),
                        'package_view_url': None
                    },
                    'data_source': 'historical_web_scrape',
                    'raw_batch_quote_data': None,
                    'scrape_timestamp': datetime.utcnow().isoformat() + 'Z'
                }
        
        return list(solicitations.values())
    
    def download_solicitation_pdfs(self, solicitations: List[Dict]) -> int:
        """Download available PDFs for historical solicitations"""
        pdf_count = 0
        pdf_skipped = 0
        
        for sol in solicitations:
            sol_num = sol['solicitation_number']
            
            # Check if we have a URL from web scrape
            sol_url = sol.get('links', {}).get('solicitation_url', '') if sol.get('links') else ''
            
            if sol_url and sol_url.endswith('.PDF'):
                # Extract filename
                match = re.search(r'/(\d+)/(SPE\w+\d+)\.PDF', sol_url, re.IGNORECASE)
                if match:
                    filename = f"{match.group(2)}.PDF"
                    gcs_path = f"{DATASET}/{sol_num}/{filename}"
                    
                    if self.dry_run:
                        logger.info(f"  [DRY RUN] Would download PDF for {sol_num} from {sol_url}")
                        logger.info(f"  [DRY RUN] Would upload to GCS: {gcs_path}")
                        pdf_count += 1
                        continue
                    
                    # Check if already exists
                    try:
                        blob = self.storage.client.bucket(BUCKET).blob(gcs_path)
                        blob.reload()
                        logger.debug(f"  PDF already exists for {sol_num}")
                        pdf_skipped += 1
                        continue
                    except:
                        pass
                    
                    # Download PDF
                    logger.debug(f"  Downloading PDF for {sol_num}")
                    try:
                        response = self.session_dibbs2.get(sol_url, timeout=60, verify=False)
                        response.raise_for_status()
                        
                        # Upload to GCS
                        self.storage.object_put(
                            bucket=BUCKET,
                            filename=gcs_path,
                            data=io.BytesIO(response.content),
                            mimetype='application/pdf'
                        )
                        
                        pdf_count += 1
                        time.sleep(0.1)  # Rate limit
                        
                    except Exception as e:
                        logger.warning(f"  Failed to download PDF for {sol_num}: {str(e)}")
        
        if self.dry_run:
            logger.info(f"  [DRY RUN] Would have downloaded {pdf_count} PDFs")
        else:
            logger.info(f"  Downloaded {pdf_count} PDFs, skipped {pdf_skipped} existing")
        
        return pdf_count
    
    def process_date(self, date_str: str) -> Dict:
        """Process a single historical date"""
        logger.info(f"\nProcessing historical data for {date_str}")
        
        results = {
            'date': date_str,
            'success': False,
            'batch_records': 0,
            'web_records': 0,
            'total_solicitations': 0,
            'pdfs_downloaded': 0,
            'bigquery_loaded': False
        }
        
        try:
            # Get batch data for this date
            batch_data = self.historical_data.get(date_str, {})
            results['batch_records'] = sum(len(records) for records in batch_data.values())
            
            if not batch_data:
                logger.info(f"  No batch data found for {date_str} in bqothers.zip")
            
            # Try web scrape
            web_data = self.scrape_historical_web_data(date_str)
            results['web_records'] = len(web_data)
            
            # Merge data sources
            merged_data = self.merge_historical_data(date_str, web_data, batch_data)
            results['total_solicitations'] = len(merged_data)
            
            if not merged_data:
                logger.warning(f"No data found for {date_str} from any source")
                results['success'] = True  # Not a failure, just no data
                return results
            
            # Download PDFs if any URLs available
            if web_data:
                results['pdfs_downloaded'] = self.download_solicitation_pdfs(merged_data)
            
            # Load to BigQuery with same table naming as combined.py
            table_id = f"SOLICITATIONS_{date_str.replace('-', '_')}"
            
            if self.dry_run:
                logger.info(f"  [DRY RUN] Would load {len(merged_data)} records to {RFQ_PROJECT}.{BQ_DATASET}.{table_id}")
                # Log sample record structure
                if merged_data:
                    logger.info("  [DRY RUN] Sample record structure:")
                    sample = merged_data[0]
                    logger.info(f"    - solicitation_number: {sample.get('solicitation_number')}")
                    logger.info(f"    - data_source: {sample.get('data_source')}")
                    logger.info(f"    - issued_date: {sample.get('issued_date')}")
                    logger.info(f"    - return_by_date: {sample.get('return_by_date')}")
                    logger.info(f"    - CLINs: {len(sample.get('clins', []))}")
                    if sample.get('clins'):
                        clin = sample['clins'][0]
                        logger.info(f"      - CLIN sample: {clin.get('clin')} - {clin.get('nsn')} - {clin.get('nomenclature')}")
                
                results['bigquery_loaded'] = True
                results['success'] = True
            else:
                try:
                    self.bq.json_to_table(
                        project_id=RFQ_PROJECT,
                        dataset_id=BQ_DATASET,
                        table_id=table_id,
                        json_data=merged_data,
                        schema=SOLICITATIONS_SCHEMA
                    )
                    
                    results['bigquery_loaded'] = True
                    results['success'] = True
                    logger.info(f"  ✓ Loaded {len(merged_data)} solicitations to BigQuery")
                    
                except Exception as e:
                    logger.error(f"  ✗ BigQuery load failed: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error processing {date_str}: {str(e)}")
            
        return results
    
    def process_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Process a range of historical dates"""
        # First load all historical data
        self.load_historical_data()
        
        if not self.historical_data:
            logger.error("No historical data loaded")
            return []
        
        # Get intersection of requested dates and available data
        all_dates = sorted(self.historical_data.keys())
        available_dates = []
        
        current = start_date
        while current <= end_date:
            date_str = current.strftime('%Y-%m-%d')
            if date_str in self.historical_data:
                available_dates.append(date_str)
            current += timedelta(days=1)
        
        logger.info(f"Found {len(available_dates)} dates with data in requested range")
        
        # Process each date
        results = []
        for idx, date_str in enumerate(available_dates, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {idx}/{len(available_dates)}: {date_str}")
            logger.info(f"{'='*60}")
            
            # Check if already processed
            table_id = f"SOLICITATIONS_{date_str.replace('-', '_')}"
            if not self.dry_run and self.bq.table_exists(RFQ_PROJECT, BQ_DATASET, table_id):
                logger.info(f"Skipping {date_str} - table already exists")
                continue
                
            result = self.process_date(date_str)
            results.append(result)
            
            # Rate limiting
            time.sleep(1)
            
        return results


def main():
    parser = argparse.ArgumentParser(
        description='DIBBS Historical Data Processor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process specific historical date
  python historical_scraper.py -p PROJECT_ID -s service.json --date 2023-06-15
  
  # Process date range
  python historical_scraper.py -p PROJECT_ID -s service.json --start-date 2023-01-01 --end-date 2023-01-31
  
  # Process all available historical data
  python historical_scraper.py -p PROJECT_ID -s service.json --all
  
  # Test mode (limit records)
  python historical_scraper.py -p PROJECT_ID -s service.json --date 2023-06-15 --test
  
  # Dry run - see what would happen without making changes
  python historical_scraper.py -p PROJECT_ID -s service.json --date 2023-06-15 --dry-run
  
  # Combine test mode and dry run for quick testing
  python historical_scraper.py -p PROJECT_ID -s service.json --date 2023-06-15 --test --dry-run
        """
    )
    
    parser.add_argument('--project', '-p', required=True, help='Google Cloud Project ID')
    parser.add_argument('--service', '-s', required=True, help='Path to service account JSON')
    parser.add_argument('--date', '-d', help='Specific date to process (YYYY-MM-DD)')
    parser.add_argument('--start-date', help='Start date for range (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for range (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Process all available historical data')
    parser.add_argument('--test', '-t', action='store_true', help='Test mode - limit records')
    parser.add_argument('--dry-run', action='store_true', help='Dry run - no data written to GCS or BigQuery')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Initialize configuration
    config = Configuration(
        project=args.project,
        service=args.service,
        verbose=args.verbose
    )
    
    # Initialize processor
    processor = HistoricalDataProcessor(config, test_mode=args.test, dry_run=args.dry_run)
    
    # Load historical data first
    processor.load_historical_data()
    
    # Determine what to process
    if args.date:
        # Single date
        result = processor.process_date(args.date)
        results = [result]
        
    elif args.start_date and args.end_date:
        # Date range
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        results = processor.process_date_range(start_date, end_date)
        
    elif args.all:
        # All available dates
        if processor.historical_data:
            all_dates = sorted(processor.historical_data.keys())
            logger.info(f"Processing all {len(all_dates)} dates from {all_dates[0]} to {all_dates[-1]}")
            
            # Process in chunks to avoid memory issues
            results = []
            for date_str in all_dates:
                # Check if already processed
                table_id = f"SOLICITATIONS_{date_str.replace('-', '_')}"
                if not processor.dry_run and processor.bq.table_exists(RFQ_PROJECT, BQ_DATASET, table_id):
                    logger.info(f"Skipping {date_str} - already processed")
                    continue
                    
                result = processor.process_date(date_str)
                results.append(result)
                time.sleep(1)
        else:
            logger.error("No historical data available")
            return 1
    else:
        parser.error("Must specify --date, --start-date/--end-date, or --all")
    
    # Summary
    print("\n" + "="*80)
    print("PROCESSING COMPLETE")
    print("="*80)
    
    if results:
        successful = sum(1 for r in results if r['success'])
        total_solicitations = sum(r['total_solicitations'] for r in results)
        total_pdfs = sum(r['pdfs_downloaded'] for r in results)
        
        print(f"Dates processed: {len(results)}")
        print(f"Successful: {successful}")
        print(f"Total solicitations: {total_solicitations}")
        print(f"PDFs downloaded: {total_pdfs}")
        
        # Show date coverage
        if processor.historical_data:
            all_dates = sorted(processor.historical_data.keys())
            print(f"\nHistorical data available from {all_dates[0]} to {all_dates[-1]}")
            print(f"Total dates with data: {len(all_dates)}")
    
    return 0


if __name__ == '__main__':
    exit(main())