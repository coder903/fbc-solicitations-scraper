#!/usr/bin/env python3
"""
DIBBS Unified Scraper - Without redundant CA file downloads
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
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
import requests
import urllib3

from util.storage_api import Storage
from util.configuration import Configuration
from util.bigquery_api import BigQuery

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import the actual parser and session functions
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from solicitations import dibbs_session, RfqRecsParser, FormParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


RFQ_PROJECT = 'argon-surf-426917-k8'

DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'
BUCKET = 'fbc-requests'
DATASET = 'DIBBS'  # For file storage structure
BQ_DATASET = 'REQUESTS'  # For BigQuery dataset

# Headers from solicitations.py
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

# BigQuery schema
SOLICITATIONS_SCHEMA = [
    {"name": "solicitation_number", "type": "STRING", "mode": "REQUIRED"},
    {"name": "solicitation_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "issued_date", "type": "DATE", "mode": "NULLABLE"},  # Moved to top level
    {"name": "return_by_date", "type": "DATETIME", "mode": "NULLABLE"},  # Already here
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
    {"name": "amsc", "type": "STRING", "mode": "NULLABLE"},  # Added AMSC field
    
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
            # Removed issued_date and return_by from here
            
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


def dibbs_solicitations_scrape(config, day, test_mode=False, max_records=None):
    """
    Scrape solicitations data for a given day - reimplemented from solicitations.py
    """
    number = 1
    rows = []

    # Activate dibbs access
    session_dibbs = dibbs_session(DIBBS_HOST)

    logger.info(f'Scraping solicitations for {day}')
    if test_mode and max_records:
        logger.info(f'TEST MODE: Limiting to {max_records} records')
    
    logger.info(f'PAGE {number}')

    # Load first page
    form_action = f'{DIBBS_HOST}RFQ/RfqRecs.aspx?category=post&TypeSrch=dt&Value={day}'
    page = session_dibbs.request('GET', form_action).content.decode('utf-8')
    
    form_data, _, form_method = FormParser().parse(page)
    count, rows_more = RfqRecsParser().parse(page)
    rows.extend(rows_more)

    logger.info(f"Found {count} total records. Retrieved {len(rows_more)} from page {number}")

    # Check if we've reached the test limit
    if test_mode and max_records and len(rows) >= max_records:
        logger.info(f'TEST MODE: Stopping at {len(rows)} records')
        return rows[:max_records]

    # Loop additional pages
    while len(rows) < count and len(rows_more) > 0:
        # Check test limit before fetching next page
        if test_mode and max_records and len(rows) >= max_records:
            logger.info(f'TEST MODE: Stopping at {len(rows)} records')
            return rows[:max_records]
            
        number += 1
        logger.info(f'PAGE {number} ({len(rows)} of {count})')

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

        page = session_dibbs.request('POST',
            url=form_action,
            data=form_data,
        ).content.decode('utf-8')

        form_data, _, form_method = FormParser().parse(page)
        _, rows_more = RfqRecsParser().parse(page)
        rows.extend(rows_more)
        
        logger.info(f"Retrieved {len(rows_more)} records from page {number}")

    return rows


class DIBBSUnifiedScraper:
    def __init__(self, config, skip_existing=True, test_mode=False):
        self.config = config
        self.storage = Storage(config, "service")
        self.bq = BigQuery(config, "service")
        self.skip_existing = skip_existing
        self.test_mode = test_mode
        self.test_limit = 30  # Limit for test mode
        
        # Create sessions
        self.session_dibbs = dibbs_session(DIBBS_HOST)
        self.session_dibbs2 = dibbs_session(DIBBS2_HOST, verify=False)

    def clean_nsn(self, nsn_value: str) -> str:
        """Clean NSN by removing text suffixes like 'MilSpec'"""
        if not nsn_value:
            return ''
        
        # Remove everything after the first space
        cleaned = nsn_value.split(' ')[0].strip()
        
        # Remove any non-numeric characters except hyphens
        # Keep hyphens for part numbers that might have them
        return cleaned

    def extract_quantity_from_purchase_request(self, purchase_request: str) -> Tuple[str, Optional[int]]:
        """Extract quantity from purchase request and return cleaned PR and quantity"""
        if not purchase_request:
            return '', None
        
        # Look for "QTY: X" pattern
        import re
        qty_pattern = r'\s*QTY:\s*(\d+(?:,\d+)*)\s*'
        match = re.search(qty_pattern, purchase_request, re.IGNORECASE)
        
        if match:
            # Extract quantity and remove commas
            qty_str = match.group(1).replace(',', '')
            quantity = int(qty_str)
            
            # Remove the QTY part from purchase request
            cleaned_pr = re.sub(qty_pattern, '', purchase_request, flags=re.IGNORECASE).strip()
            
            return cleaned_pr, quantity
        
        return purchase_request.strip(), None

    def clean_nomenclature(self, nomenclature: str) -> str:
        """Clean nomenclature field"""
        if not nomenclature:
            return ''
        
        # Remove extra spaces and normalize
        return ' '.join(nomenclature.split())

        
    def check_date_exists(self, date_str: str) -> bool:
        """Check if date already exists in BigQuery"""
        if not self.skip_existing:
            return False
            
        table_name = f"SOLICITATIONS_{date_str.replace('-', '_')}"
        # Check in RFQ project
        exists = self.bq.table_exists(RFQ_PROJECT, BQ_DATASET, table_name)
        
        if exists:
            logger.info(f"Table {RFQ_PROJECT}.{BQ_DATASET}.{table_name} already exists")
            # For today's date, always return False to allow updates
            if date_str == datetime.now().strftime('%Y-%m-%d'):
                logger.info("  Processing today's date - will check for new records")
                return False
            
            # For historical dates, check if table has data
            query = f"SELECT COUNT(*) as count FROM `{RFQ_PROJECT}.{BQ_DATASET}.{table_name}`"
            try:
                result = self.bq.query(query)
                if result and result[0]['count'] > 0:
                    return True
            except:
                pass
                
        return False

    def get_existing_solicitations(self, date_str: str) -> set:
        """Get list of solicitations already processed for a date"""
        table_name = f"SOLICITATIONS_{date_str.replace('-', '_')}"
        existing_solicitations = set()
        
        try:
            query = f"""
            SELECT DISTINCT solicitation_number 
            FROM `{RFQ_PROJECT}.{BQ_DATASET}.{table_name}`
            """
            results = self.bq.query(query)
            if results:
                existing_solicitations = {row['solicitation_number'] for row in results}
                logger.info(f"  Found {len(existing_solicitations)} existing solicitations in BigQuery")
        except Exception as e:
            logger.debug(f"  No existing data found: {str(e)}")
        
        return existing_solicitations

    def file_exists_in_gcs(self, filepath: str) -> bool:
        """Check if file exists in GCS"""
        try:
            blob = self.storage.client.bucket(BUCKET).blob(filepath)
            blob.reload()
            return True
        except Exception:
            return False

    def should_skip_pdf_download(self, sol_num: str, filename: str) -> bool:
        """Check if PDF already exists in GCS"""
        gcs_path = f"{DATASET}/{sol_num}/{filename}"
        return self.file_exists_in_gcs(gcs_path)

    def should_skip_tech_doc_download(self, sol_num: str, filename: str) -> bool:
        """Check if technical document already exists in GCS"""
        gcs_path = f"{DATASET}/{sol_num}/technical_docs/{filename}"
        return self.file_exists_in_gcs(gcs_path)
    
    def download_to_memory(self, url: str, session=None) -> Optional[bytes]:
        """Download file to memory"""
        if session is None:
            session = self.session_dibbs2 if 'dibbs2' in url else self.session_dibbs
            
        try:
            response = session.get(url, timeout=60, verify=False)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return None
    
    def parse_in_file(self, content: str) -> List[Dict]:
        """Parse IN file content"""
        records = []
        
        for line in content.split('\n'):
            if len(line.strip()) == 0:
                continue
                
            # Parse fixed-width format
            try:
                record = {
                    'solicitation_number': line[0:13].strip(),
                    'nsn_part_number': line[13:59].strip(),
                    'purchase_request': line[59:72].strip(),
                    'return_by_date': line[72:80].strip(),
                    'file_name': line[80:99].strip(),
                    'quantity': line[99:106].strip(),
                    'unit_issue': line[106:108].strip(),
                    'nomenclature': line[108:129].strip(),
                    'buyer_code': line[129:134].strip() if len(line) > 129 else '',
                    'amsc': line[134:135].strip() if len(line) > 134 else '',
                    'item_type': line[135:136].strip() if len(line) > 135 else '',
                    'small_business_setaside': line[136:137].strip() if len(line) > 136 else '',
                    'setaside_percentage': line[137:140].strip() if len(line) > 137 else ''
                }
                
                # DEBUG: Log setaside info for first few records
                if len(records) < 5:
                    logger.debug(f"IN file record {record['solicitation_number']}: "
                            f"setaside={record.get('small_business_setaside', 'N')}, "
                            f"percentage={record.get('setaside_percentage', '')}")
                
                # Clean up NSN
                if record['nsn_part_number']:
                    record['nsn_clean'] = record['nsn_part_number'].replace('-', '')
                
                records.append(record)
            except Exception as e:
                logger.debug(f"Error parsing IN line: {str(e)}")
                
        return records
    
    def parse_as_file(self, content: str) -> List[Dict]:
        """Parse AS file content"""
        approved_sources = []
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Parse CSV format
            parts = [p.strip('"') for p in line.split(',')]
            if len(parts) >= 3:
                source = {
                    'nsn': parts[0],
                    'supplier_cage_code': parts[1],
                    'supplier_part_number': parts[2],
                    'supplier_name': parts[3] if len(parts) > 3 else ''
                }
                approved_sources.append(source)
                
        return approved_sources
    
    def parse_bq_file(self, content: str) -> List[Dict]:
        """Parse BQ file content"""
        batch_quotes = []
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                continue
                
            # Parse CSV format with many fields
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
            
            # Map to fields based on batch quote format
            if len(parts) >= 48:  # Minimum required fields
                quote = {
                    'solicitation_number': parts[0].strip('"'),
                    'solicitation_type': parts[1].strip('"'),
                    'small_business_setaside': parts[2].strip('"'),
                    'additional_clause_fillins': parts[3].strip('"'),
                    'return_by_date': parts[4].strip('"'),
                    'line_number': parts[43].strip('"') if len(parts) > 43 else '',
                    'purchase_request': parts[45].strip('"') if len(parts) > 45 else '',
                    'nsn_part_number': parts[46].strip('"') if len(parts) > 46 else '',
                    'unit_of_issue': parts[47].strip('"') if len(parts) > 47 else '',
                    'quantity': parts[48].strip('"') if len(parts) > 48 else '',
                    'unit_price': parts[49].strip('"') if len(parts) > 49 else '',
                    'delivery_days': parts[50].strip('"') if len(parts) > 50 else '',
                }
                
                # Clean NSN
                if quote['nsn_part_number']:
                    quote['nsn_clean'] = quote['nsn_part_number'].replace('-', '')
                    
                batch_quotes.append(quote)
                
        return batch_quotes
    
    def decode_setaside(self, code: str) -> Dict:
        """Decode small business set-aside indicator"""
        mapping = {
            'Y': ('Small Business Set-Aside', 100),
            'H': ('HUBZone Set-Aside', 100),
            'R': ('Service Disabled Veteran-Owned Small Business (SDVOSB) Set-Aside', 100),
            'L': ('Woman Owned Small Business (WOSB) Set-Aside', 100),
            'A': ('8a Set-Aside', 100),
            'E': ('Economically Disadvantaged Woman Owned Small Business (EDWOSB) Set-Aside', 100),
            'N': ('Unrestricted/Not Set-Aside', 0),
        }
        
        if code in mapping:
            return {
                'code': code,
                'description': mapping[code][0],
                'percentage': mapping[code][1]
            }
        return {'code': code, 'description': 'Unknown', 'percentage': 0}
    
    def parse_date(self, date_str: str, format: str = '%m/%d/%Y') -> Optional[str]:
        """Parse date string to BigQuery format"""
        if not date_str or date_str.strip() == '':
            return None
            
        try:
            # Handle MM-DD-YYYY format from web scrape
            if '-' in date_str and len(date_str.split('-')[0]) == 2:
                format = '%m-%d-%Y'
            
            formats = [format, '%m-%d-%Y', '%Y-%m-%d', '%m/%d/%y', '%m%d%Y', '%m%d%y']
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str.strip(), fmt)
                    return dt.strftime('%Y-%m-%d')
                except:
                    continue
            return None
        except:
            return None
    


    def merge_data_sources(self, web_data: List[Dict], in_data: List[Dict], 
                    bq_data: List[Dict], as_data: List[Dict]) -> List[Dict]:
        """Merge data from different sources into unified structure"""
        
        # Add debug counters
        debug_stats = {
            'web_solicitations': set(),
            'bq_solicitations': set(),
            'matched_solicitations': set(),
            'web_only': set(),
            'bq_only': set()
        }
        
        # Create lookup dictionaries
        in_lookup = {record['solicitation_number']: record for record in in_data}
        bq_lookup = {}
        for record in bq_data:
            key = (record['solicitation_number'], record.get('line_number', ''))
            if key not in bq_lookup:
                bq_lookup[key] = []
            bq_lookup[key].append(record)
            debug_stats['bq_solicitations'].add(record['solicitation_number'])
        
        # Track web solicitations
        for record in web_data:
            sol_num = record.get('solicitation', '').strip()
            if sol_num:
                debug_stats['web_solicitations'].add(sol_num)
        
        # Create AS lookup by NSN
        as_lookup = {}
        for record in as_data:
            nsn = record['nsn'].replace('-', '')
            if nsn not in as_lookup:
                as_lookup[nsn] = []
            as_lookup[nsn].append(record)
        
        # Process each solicitation
        solicitations = {}
        
        # Start with BQ data first (has proper CLIN numbers)
        for key, records in bq_lookup.items():
            sol_num = key[0]
            
            # Clean the solicitation number (remove any suffixes)
            sol_num_clean = sol_num.strip().upper()
            
            if sol_num_clean not in solicitations:
                solicitations[sol_num_clean] = {
                    'solicitation_number': sol_num_clean,
                    'clins': [],
                    'scrape_timestamp': datetime.utcnow().isoformat() + 'Z',
                    'data_source': 'batch_quote',
                    'last_updated': datetime.utcnow().isoformat() + 'Z'
                }
            
            sol = solicitations[sol_num_clean]
            
            # Use first record for header data
            if records:
                first = records[0]
                sol.update({
                    'solicitation_type': first.get('solicitation_type'),
                    'small_business_setaside': first.get('small_business_setaside'),
                    'additional_clause_fillins': first.get('additional_clause_fillins') == 'Y',
                    'return_by_date': self.parse_date(first.get('return_by_date')),
                })
            
            # Process each line item with cleaning
            for record in records:
                # NSN is the government number (cleaned, no dashes)
                nsn_raw = record.get('nsn_part_number', '')
                nsn_clean = nsn_raw.replace('-', '') if nsn_raw else ''
                
                # Part number initially same as raw value (will be updated from approved sources)
                part_number = nsn_raw  # Keep original format with dashes if present
                
                # Use proper CLIN number from BQ data
                clin_number = record.get('line_number', '').zfill(4)  # Ensure 4 digits with leading zeros
                
                clin = {
                    'clin': clin_number,
                    'nsn': nsn_clean,  # Clean NSN without dashes
                    'part_number': part_number,  # Will be updated from approved sources
                    'nomenclature': self.clean_nomenclature(record.get('nomenclature', '')),
                    'pr_number': record.get('purchase_request'),
                    'quantity': self._parse_int(record.get('quantity')),
                    'unit_of_issue': record.get('unit_of_issue'),
                    'unit_price': self._parse_float(record.get('unit_price')),
                    'delivery_days': self._parse_int(record.get('delivery_days')),
                    'approved_sources': []
                }
                sol['clins'].append(clin)
                
                # Store raw BQ data
                if 'raw_batch_quote_data' not in sol:
                    sol['raw_batch_quote_data'] = []
                sol['raw_batch_quote_data'].append(record)
        
        # Then process web scrape data (enhance existing solicitations or add new ones)
        for record in web_data:
            sol_num = record.get('solicitation', '').strip()
            if not sol_num:
                continue
            
            # Clean the solicitation number to match BQ format
            sol_num_clean = sol_num.strip().upper()
            
            # Debug: Log the matching attempt
            if sol_num_clean in solicitations:
                debug_stats['matched_solicitations'].add(sol_num_clean)
                logger.debug(f"MATCHED: Web solicitation {sol_num} -> {sol_num_clean}")
            else:
                debug_stats['web_only'].add(sol_num_clean)
                logger.debug(f"NOT MATCHED: Web solicitation {sol_num} -> {sol_num_clean}")
                
            if sol_num_clean not in solicitations:
                solicitations[sol_num_clean] = {
                    'solicitation_number': sol_num_clean,
                    'clins': [],
                    'scrape_timestamp': datetime.utcnow().isoformat() + 'Z',
                    'data_source': 'web_scrape',
                    'last_updated': datetime.utcnow().isoformat() + 'Z'
                }
            
            sol = solicitations[sol_num_clean]
            
            # Update data source to indicate both sources
            if sol['data_source'] == 'batch_quote':
                sol['data_source'] = 'batch_quote_and_web_scrape'
            
            # Add web scrape specific fields INCLUDING DATES AT SOLICITATION LEVEL
            sol.update({
                'setaside_type': record.get('setaside_type'),
                'rfq_quote_status': record.get('rfq_quote_status'),
                'issued_date': self.parse_date(record.get('issued', '')),  # Now at solicitation level
                'links': {
                    'solicitation_url': record.get('solicitation_url'),
                }
            })
            
            # Debug: Log if issued_date was set
            if sol.get('issued_date'):
                logger.debug(f"Set issued_date for {sol_num_clean}: {sol['issued_date']}")
            else:
                logger.debug(f"No issued_date for {sol_num_clean}, raw value was: {record.get('issued', 'MISSING')}")
            
            # Update return_by_date if not already set
            if not sol.get('return_by_date'):
                sol['return_by_date'] = self.parse_date(record.get('return_by', ''))
            
            # Clean and extract data
            raw_nsn = record.get('nsn_part_number', '')
            clean_nsn = self.clean_nsn(raw_nsn)
            clean_pr, extracted_qty = self.extract_quantity_from_purchase_request(
                record.get('purchase_request', '')
            )
            clean_nomenclature = self.clean_nomenclature(record.get('nomenclature', ''))
            
            # Use extracted quantity if available, otherwise use original quantity
            final_quantity = extracted_qty if extracted_qty is not None else self._parse_int(record.get('quantity'))
            
            # Try to match with existing CLIN by NSN
            existing_clin = None
            for clin in sol['clins']:
                if clin['nsn'] == clean_nsn:
                    existing_clin = clin
                    break
            
            if existing_clin:
                # Enhance existing CLIN with web scrape data
                existing_clin.update({
                    'technical_documents': record.get('technical_documents', [])
                })
                # Only update fields if they're missing from BQ data
                if not existing_clin.get('nomenclature'):
                    existing_clin['nomenclature'] = clean_nomenclature
                if not existing_clin.get('pr_number'):
                    existing_clin['pr_number'] = clean_pr
                if not existing_clin.get('quantity'):
                    existing_clin['quantity'] = final_quantity
            else:
                # Create new CLIN without CLIN number (web scrape doesn't have reliable CLIN numbers)
                new_clin = {
                    'clin': None,
                    'nsn': clean_nsn,  # Clean NSN
                    'part_number': raw_nsn,  # Keep original value
                    'nomenclature': clean_nomenclature,
                    'pr_number': clean_pr,
                    'quantity': final_quantity,
                    'technical_documents': record.get('technical_documents', [])
                }
                sol['clins'].append(new_clin)
        
        # Process IN file data (fill in missing fields)
        for record in in_data:
            sol_num = record['solicitation_number']
            
            if sol_num not in solicitations:
                solicitations[sol_num] = {
                    'solicitation_number': sol_num,
                    'clins': [],
                    'scrape_timestamp': datetime.utcnow().isoformat() + 'Z',
                    'data_source': 'batch_files',
                    'last_updated': datetime.utcnow().isoformat() + 'Z'
                }
            
            sol = solicitations[sol_num]
            
            # Add IN file specific data INCLUDING AMSC
            setaside = self.decode_setaside(record.get('small_business_setaside', 'N'))
            
            # Parse the percentage from IN file
            setaside_pct_str = record.get('setaside_percentage', '').strip()
            if setaside_pct_str and setaside_pct_str.isdigit():
                setaside_pct = float(setaside_pct_str)
            else:
                # Use default from decode_setaside
                setaside_pct = setaside['percentage']
            
            # Update solicitation with IN file data
            sol.update({
                'small_business_setaside': setaside['code'],
                'setaside_percentage': setaside_pct,
                'setaside_type': setaside['description'],  # This gives you the descriptive text
                'return_by_date': self.parse_date(record.get('return_by_date'), '%m%d%y'),
                'amsc': record.get('amsc', '')
            })
            
            # Try to enhance existing CLINs with IN file data
            clean_nsn = self.clean_nsn(record.get('nsn_part_number', ''))
            nsn_clean = record.get('nsn_clean', clean_nsn)
            
            existing_clin = None
            for clin in sol['clins']:
                if clin['nsn'] == nsn_clean:
                    existing_clin = clin
                    break
            
            if existing_clin:
                # Enhance with IN file data if missing
                if not existing_clin.get('nomenclature'):
                    existing_clin['nomenclature'] = self.clean_nomenclature(record.get('nomenclature', ''))
                if not existing_clin.get('pr_number'):
                    existing_clin['pr_number'] = record.get('purchase_request')
                if not existing_clin.get('quantity'):
                    existing_clin['quantity'] = self._parse_int(record.get('quantity'))
                if not existing_clin.get('unit_of_issue'):
                    existing_clin['unit_of_issue'] = record.get('unit_issue')
            else:
                # Create new CLIN from IN file data
                new_clin = {
                    'clin': None,
                    'nsn': nsn_clean,  # Clean NSN without dashes
                    'part_number': record.get('nsn_part_number'),  # Keep original value
                    'nomenclature': self.clean_nomenclature(record.get('nomenclature', '')),
                    'pr_number': record.get('purchase_request'),
                    'quantity': self._parse_int(record.get('quantity')),
                    'unit_of_issue': record.get('unit_issue')
                }
                sol['clins'].append(new_clin)
        
        # Add approved sources to CLINs and update part numbers with vendor part numbers
        for sol in solicitations.values():
            for clin in sol['clins']:
                nsn = clin.get('nsn', '')
                if nsn and nsn in as_lookup:
                    # Get approved sources
                    approved_sources = [
                        {
                            'supplier_cage_code': source['supplier_cage_code'],
                            'supplier_part_number': source['supplier_part_number'],
                        }
                        for source in as_lookup[nsn]
                    ]
                    clin['approved_sources'] = approved_sources
                    
                    # Update part_number to vendor part number if we have approved sources
                    # and current part_number looks like an NSN (all digits or with dashes)
                    if approved_sources:
                        current_part = clin.get('part_number', '').replace('-', '')
                        # If current part number is all digits (likely NSN), replace with vendor part
                        if current_part.isdigit() or not clin.get('part_number'):
                            clin['part_number'] = approved_sources[0]['supplier_part_number']
        
        # Calculate unmatched
        debug_stats['bq_only'] = debug_stats['bq_solicitations'] - debug_stats['matched_solicitations']
        
        # Print debug summary
        logger.info("\n=== MERGE STATISTICS ===")
        logger.info(f"Web solicitations: {len(debug_stats['web_solicitations'])}")
        logger.info(f"BQ solicitations: {len(debug_stats['bq_solicitations'])}")
        logger.info(f"Matched (both sources): {len(debug_stats['matched_solicitations'])}")
        logger.info(f"Web only: {len(debug_stats['web_only'])}")
        logger.info(f"BQ only: {len(debug_stats['bq_only'])}")
        
        # Show some examples of unmatched
        if debug_stats['bq_only']:
            logger.info(f"\nExample BQ solicitations without web match (first 5):")
            for sol in list(debug_stats['bq_only'])[:5]:
                logger.info(f"  - {sol}")
        
        if debug_stats['web_only']:
            logger.info(f"\nExample Web solicitations without BQ match (first 5):")
            for sol in list(debug_stats['web_only'])[:5]:
                logger.info(f"  - {sol}")
        
        return list(solicitations.values())
    
    def _parse_int(self, value) -> Optional[int]:
        """Safely parse integer value"""
        if value is None or value == '':
            return None
        try:
            # Remove commas if present
            cleaned = str(value).strip().replace(',', '')
            return int(cleaned)
        except:
            return None
    
    def _parse_float(self, value) -> Optional[float]:
        """Safely parse float value"""
        if value is None or value == '':
            return None
        try:
            # Remove commas if present
            cleaned = str(value).strip().replace(',', '')
            return float(cleaned)
        except:
            return None
    
    def process_date(self, date_str: str, date_code: str) -> Dict:
        """Process all data for a single date"""
        # Convert date format for web scrape (MM-DD-YYYY)
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        web_date_str = date_obj.strftime('%m-%d-%Y')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {date_str} (web format: {web_date_str})")
        logger.info(f"{'='*60}")
        
        results = {
            'date': date_str,
            'success': False,
            'solicitations_count': 0,
            'files_uploaded': 0,
            'bigquery_loaded': False,
            'warnings': []
        }
        
        try:
            # Step 1: Web scrape solicitations
            logger.info("Step 1: Web scraping solicitations...")
            if self.test_mode:
                logger.info(f"  TEST MODE: Limiting to {self.test_limit} records")
            
            web_data = dibbs_solicitations_scrape(
                self.config, 
                web_date_str, 
                test_mode=self.test_mode,
                max_records=self.test_limit if self.test_mode else None
            )
            results['solicitations_count'] = len(web_data)
            logger.info(f"  Found {len(web_data)} solicitations")
            
            if len(web_data) == 0:
                results['warnings'].append("No solicitations found via web scrape")
            
            # Check for existing solicitations if this is today's date
            existing_solicitations = set()
            is_today = date_str == datetime.now().strftime('%Y-%m-%d')
            if is_today:
                existing_solicitations = self.get_existing_solicitations(date_str)
                
                # Filter out already processed solicitations
                new_web_data = []
                for sol in web_data:
                    sol_num = sol.get('solicitation', '').strip()
                    if sol_num and sol_num not in existing_solicitations:
                        new_web_data.append(sol)
                
                logger.info(f"  Found {len(new_web_data)} NEW solicitations to process")
                if len(new_web_data) == 0:
                    logger.info("  No new solicitations found - all already processed")
                    results['success'] = True
                    results['warnings'].append("No new solicitations to process")
                    return results
                
                # Use only new solicitations for processing
                web_data = new_web_data
            
            if len(web_data) == 0:
                results['warnings'].append("No solicitations found via web scrape")
            
            # Build solicitation -> PR mapping from web data
            sol_pr_map = {}
            for sol in web_data:
                sol_num = sol.get('solicitation', '').strip()
                pr_num = sol.get('pr_number', '').strip()
                
                if sol_num and pr_num:
                    if sol_num not in sol_pr_map:
                        sol_pr_map[sol_num] = set()
                    sol_pr_map[sol_num].add(pr_num)
            
            # Also build PR -> solicitation mapping for cross-reference
            pr_sol_map = {}
            for sol_num, pr_nums in sol_pr_map.items():
                for pr_num in pr_nums:
                    if pr_num not in pr_sol_map:
                        pr_sol_map[pr_num] = set()
                    pr_sol_map[pr_num].add(sol_num)
            
            # Step 2: Download and parse IN file
            logger.info("Step 2: Downloading IN file...")
            in_data = []
            in_url = f"{DIBBS2_HOST}Downloads/RFQ/Archive/in{date_code}.txt"
            in_content = self.download_to_memory(in_url)
            
            if in_content:
                in_text = in_content.decode('utf-8', errors='ignore')
                in_data = self.parse_in_file(in_text)
                logger.info(f"  Parsed {len(in_data)} IN records")
                
                # Store IN file content for later upload to solicitation/PR folders
                results['in_content'] = in_content
            else:
                results['warnings'].append("IN file not available")
            
            # Step 3: Download and extract BQ/AS files
            logger.info("Step 3: Downloading BQ/AS files...")
            bq_data = []
            as_data = []
            bq_url = f"{DIBBS2_HOST}Downloads/RFQ/Archive/bq{date_code}.zip"
            bq_content = self.download_to_memory(bq_url)
            
            if bq_content:
                try:
                    with zipfile.ZipFile(io.BytesIO(bq_content)) as zf:
                        # Extract and parse BQ file
                        if f'bq{date_code}.txt' in zf.namelist():
                            bq_text = zf.read(f'bq{date_code}.txt').decode('utf-8', errors='ignore')
                            bq_data = self.parse_bq_file(bq_text)
                            logger.info(f"  Parsed {len(bq_data)} BQ records")
                            # Store for later upload
                            results['bq_content'] = bq_text.encode('utf-8')
                        
                        # Extract and parse AS file
                        if f'as{date_code}.txt' in zf.namelist():
                            as_text = zf.read(f'as{date_code}.txt').decode('utf-8', errors='ignore')
                            as_data = self.parse_as_file(as_text)
                            logger.info(f"  Parsed {len(as_data)} AS records")
                            # Store for later upload
                            results['as_content'] = as_text.encode('utf-8')
                                            
                except Exception as e:
                    logger.error(f"Error processing BQ zip: {str(e)}")
            else:
                results['warnings'].append("BQ/AS files not available")
            
            # Step 4: Download individual solicitation PDFs and technical documents from web scrape
            if len(web_data) > 0:
                logger.info("Step 4: Downloading PDFs from web scrape...")
                pdf_count = 0
                tech_doc_count = 0
                pdf_errors = 0
                tech_doc_errors = 0
                pdf_skipped = 0
                tech_doc_skipped = 0
                total_bytes_downloaded = 0
                
                # Progress tracking
                total_solicitations = len(web_data)
                processed_solicitations = set()  # Track which solicitations we've already processed
                
                for idx, sol in enumerate(web_data, 1):
                    sol_num = sol.get('solicitation', '')
                    
                    # Log progress every 10 solicitations or at the end
                    if idx % 10 == 0 or idx == total_solicitations:
                        logger.info(f"  Progress: {idx}/{total_solicitations} records processed")
                    
                    # Download main solicitation PDF only once per solicitation
                    if sol_num and sol_num not in processed_solicitations:
                        processed_solicitations.add(sol_num)
                        
                        sol_url = sol.get('solicitation_url', '')
                        if sol_url and sol_url.endswith('.PDF'):
                            # Extract filename from URL
                            match = re.search(r'/(\d+)/(SPE\w+\d+)\.PDF', sol_url, re.IGNORECASE)
                            if match:
                                filename = f"{match.group(2)}.PDF"
                                
                                # Check if already exists
                                if self.should_skip_pdf_download(sol_num, filename):
                                    logger.debug(f"  Skipping existing PDF for {sol_num}: {filename}")
                                    pdf_skipped += 1
                                    results['files_uploaded'] += 1  # Count as uploaded since it exists
                                    continue
                                
                                logger.debug(f"  Downloading solicitation PDF for {sol_num}: {sol_url}")
                                start_time = time.time()
                                
                                pdf_content = self.download_to_memory(sol_url, self.session_dibbs2)
                                
                                if pdf_content:
                                    download_time = time.time() - start_time
                                    pdf_size_mb = len(pdf_content) / (1024 * 1024)
                                    total_bytes_downloaded += len(pdf_content)
                                    
                                    logger.debug(f"    Downloaded {pdf_size_mb:.2f}MB in {download_time:.2f}s")
                                    
                                    gcs_path = f"{DATASET}/{sol_num}/{filename}"
                                    
                                    try:
                                        logger.debug(f"    Uploading to GCS: {gcs_path}")
                                        upload_start = time.time()
                                        
                                        self.storage.object_put(
                                            bucket=BUCKET,
                                            filename=gcs_path,
                                            data=io.BytesIO(pdf_content),
                                            mimetype='application/pdf'
                                        )
                                        
                                        upload_time = time.time() - upload_start
                                        logger.debug(f"    Upload completed in {upload_time:.2f}s")
                                        
                                        results['files_uploaded'] += 1
                                        pdf_count += 1
                                        
                                        # Add small delay to avoid rate limits
                                        time.sleep(0.1)
                                    except Exception as e:
                                        logger.error(f"    Error uploading solicitation PDF for {sol_num}: {str(e)}")
                                        pdf_errors += 1
                                else:
                                    logger.warning(f"    Failed to download PDF for {sol_num}")
                                    pdf_errors += 1
                        
                        # Download technical documents (also once per solicitation)
                        tech_docs = sol.get('technical_documents', [])
                        if tech_docs:
                            logger.debug(f"  Processing {len(tech_docs)} technical documents for {sol_num}")
                            
                        for doc_idx, tech_doc in enumerate(tech_docs, 1):
                            doc_url = tech_doc.get('url', '')
                            doc_title = tech_doc.get('title', 'document')
                            
                            if doc_url:
                                # Create filename from title
                                safe_title = doc_title.replace(' ', '_').replace('/', '-')
                                filename = f"{sol_num}_{safe_title}.pdf"
                                
                                # Check if already exists
                                if self.should_skip_tech_doc_download(sol_num, filename):
                                    logger.debug(f"    Skipping existing tech doc: {filename}")
                                    tech_doc_skipped += 1
                                    results['files_uploaded'] += 1  # Count as uploaded since it exists
                                    continue
                                
                                logger.debug(f"    Downloading tech doc {doc_idx}/{len(tech_docs)}: {doc_title}")
                                start_time = time.time()
                                
                                doc_content = self.download_to_memory(doc_url, self.session_dibbs2)
                                
                                if doc_content:
                                    download_time = time.time() - start_time
                                    doc_size_mb = len(doc_content) / (1024 * 1024)
                                    total_bytes_downloaded += len(doc_content)
                                    
                                    logger.debug(f"      Downloaded {doc_size_mb:.2f}MB in {download_time:.2f}s")
                                    
                                    gcs_path = f"{DATASET}/{sol_num}/technical_docs/{filename}"
                                    
                                    try:
                                        logger.debug(f"      Uploading to GCS: {gcs_path}")
                                        upload_start = time.time()
                                        
                                        self.storage.object_put(
                                            bucket=BUCKET,
                                            filename=gcs_path,
                                            data=io.BytesIO(doc_content),
                                            mimetype='application/pdf'
                                        )
                                        
                                        upload_time = time.time() - upload_start
                                        logger.debug(f"      Upload completed in {upload_time:.2f}s")
                                        
                                        results['files_uploaded'] += 1
                                        tech_doc_count += 1
                                        
                                        # Add small delay to avoid rate limits
                                        time.sleep(0.1)
                                    except Exception as e:
                                        logger.error(f"      Error uploading tech doc '{doc_title}' for {sol_num}: {str(e)}")
                                        tech_doc_errors += 1
                                else:
                                    logger.warning(f"      Failed to download tech doc '{doc_title}' for {sol_num}")
                                    tech_doc_errors += 1
                
                # Final summary for Step 4
                total_mb = total_bytes_downloaded / (1024 * 1024)
                logger.info(f"  Step 4 Complete:")
                logger.info(f"    Solicitation PDFs: {pdf_count} downloaded, {pdf_skipped} skipped, {pdf_errors} errors")
                logger.info(f"    Technical documents: {tech_doc_count} downloaded, {tech_doc_skipped} skipped, {tech_doc_errors} errors")
                logger.info(f"    Total data downloaded: {total_mb:.2f}MB")
                logger.info(f"    Total files in GCS: {pdf_count + tech_doc_count + pdf_skipped + tech_doc_skipped}")
            
            # Step 5: Upload IN/AS/BQ files to solicitation/PR folders
            logger.info("Step 5: Uploading IN/AS/BQ files to solicitation/PR folders...")
            if 'in_content' in results and 'bq_content' in results and 'as_content' in results:
                pr_upload_count = 0
                
                # For each solicitation, upload files to all its PR folders
                for sol_num, pr_nums in sol_pr_map.items():
                    for pr_num in pr_nums:
                        # Check if files already exist to make it reentrant
                        in_path = f"{DATASET}/{sol_num}/{pr_num}/in{date_code}.txt"
                        as_path = f"{DATASET}/{sol_num}/{pr_num}/as{date_code}.txt"
                        bq_path = f"{DATASET}/{sol_num}/{pr_num}/bq{date_code}.txt"
                        
                        # Upload IN file if not exists
                        if not self.file_exists_in_gcs(in_path):
                            try:
                                self.storage.object_put(
                                    bucket=BUCKET,
                                    filename=in_path,
                                    data=io.BytesIO(results['in_content']),
                                    mimetype='text/plain'
                                )
                                results['files_uploaded'] += 1
                                logger.debug(f"  Uploaded IN file to {in_path}")
                            except Exception as e:
                                logger.error(f"  Error uploading IN file to {in_path}: {str(e)}")
                        else:
                            logger.debug(f"  IN file already exists: {in_path}")
                        
                        # Upload AS file if not exists
                        if not self.file_exists_in_gcs(as_path):
                            try:
                                self.storage.object_put(
                                    bucket=BUCKET,
                                    filename=as_path,
                                    data=io.BytesIO(results['as_content']),
                                    mimetype='text/plain'
                                )
                                results['files_uploaded'] += 1
                                logger.debug(f"  Uploaded AS file to {as_path}")
                            except Exception as e:
                                logger.error(f"  Error uploading AS file to {as_path}: {str(e)}")
                        else:
                            logger.debug(f"  AS file already exists: {as_path}")
                        
                        # Upload BQ file if not exists
                        if not self.file_exists_in_gcs(bq_path):
                            try:
                                self.storage.object_put(
                                    bucket=BUCKET,
                                    filename=bq_path,
                                    data=io.BytesIO(results['bq_content']),
                                    mimetype='text/plain'
                                )
                                results['files_uploaded'] += 1
                                logger.debug(f"  Uploaded BQ file to {bq_path}")
                            except Exception as e:
                                logger.error(f"  Error uploading BQ file to {bq_path}: {str(e)}")
                        else:
                            logger.debug(f"  BQ file already exists: {bq_path}")
                        
                        pr_upload_count += 1
                
                logger.info(f"  Processed {len(sol_pr_map)} solicitations with {pr_upload_count} total PR folders")
                logger.info(f"  PR to solicitation mapping shows {len(pr_sol_map)} unique PRs")
            
            # Step 6: Merge all data sources
            logger.info("Step 6: Merging data sources...")
            merged_data = self.merge_data_sources(web_data, in_data, bq_data, as_data)
            logger.info(f"  Merged into {len(merged_data)} unique solicitations")
            
            # Step 7: Load to BigQuery
            if merged_data:
                logger.info("Step 7: Loading to BigQuery...")
                # Convert raw_batch_quote_data to JSON string
                for record in merged_data:
                    if 'raw_batch_quote_data' in record and isinstance(record['raw_batch_quote_data'], list):
                        record['raw_batch_quote_data'] = json.dumps(record['raw_batch_quote_data'])
                
                # Load to BigQuery using RFQ project
                table_id = f"SOLICITATIONS_{date_str.replace('-', '_')}"
                
                try:
                    # For today's date, append to existing table
                    if is_today and existing_solicitations:
                        logger.info("  Appending new records to existing table...")
                        self.bq.json_to_table(
                            project_id=RFQ_PROJECT,
                            dataset_id=BQ_DATASET,
                            table_id=table_id,
                            json_data=merged_data,
                            schema=SOLICITATIONS_SCHEMA,
                            write_disposition='WRITE_APPEND'  # Append mode
                        )
                    else:
                        # For historical dates or first run of the day, create/replace table
                        self.bq.json_to_table(
                            project_id=RFQ_PROJECT,
                            dataset_id=BQ_DATASET,
                            table_id=table_id,
                            json_data=merged_data,
                            schema=SOLICITATIONS_SCHEMA
                        )
                    
                    results['bigquery_loaded'] = True
                    logger.info(f"  ✓ Successfully loaded {len(merged_data)} records to {RFQ_PROJECT}.{BQ_DATASET}.{table_id}")
                    
                except Exception as e:
                    logger.error(f"  ✗ Error loading to BigQuery: {str(e)}")
            else:
                logger.warning("No data to load to BigQuery")
                if len(web_data) == 0 and len(in_data) == 0 and len(bq_data) == 0:
                    results['warnings'].append("No data found from any source")
            
            # Mark as success if we processed something
            results['success'] = True
            
        except Exception as e:
            logger.error(f"Error processing {date_str}: {str(e)}")
            results['success'] = False
            
        # Summary for this date
        logger.info(f"\nSummary for {date_str}:")
        logger.info(f"  Solicitations found: {results['solicitations_count']}")
        if is_today and existing_solicitations:
            logger.info(f"  New solicitations processed: {len(web_data)}")
        logger.info(f"  Files uploaded: {results['files_uploaded']}")
        logger.info(f"  BigQuery loaded: {'Yes' if results['bigquery_loaded'] else 'No'}")
        logger.info(f"  Overall success: {'Yes' if results['success'] else 'No'}")
        
        if results['warnings']:
            logger.warning(f"  Warnings:")
            for warning in results['warnings']:
                logger.warning(f"    - {warning}")
        
        return results
    
    def process_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Process a range of dates"""
        all_results = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            date_code = current_date.strftime('%y%m%d')
            
            # Check if already processed
            if self.check_date_exists(date_str):
                logger.info(f"Skipping {date_str} - already exists in BigQuery")
                current_date += timedelta(days=1)
                continue
            
            # Process the date
            result = self.process_date(date_str, date_code)
            all_results.append(result)
            
            # Rate limiting
            time.sleep(2)
            
            current_date += timedelta(days=1)
        
        return all_results


def main():
    parser = argparse.ArgumentParser(
        description='DIBBS Unified Scraper - Combined crawler and BigQuery loader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process today
  python combined.py -p PROJECT_ID -s service.json
  
  # Process specific date
  python combined.py -p PROJECT_ID -s service.json --date 2025-06-03
  
  # Process date range
  python combined.py -p PROJECT_ID -s service.json --start-date 2025-06-01 --end-date 2025-06-03
  
  # Force reprocess
  python combined.py -p PROJECT_ID -s service.json --date 2025-06-03 --force
  
  # Test mode (limit to 30 records)
  python combined.py -p PROJECT_ID -s service.json --date 2025-06-03 --test
        """
    )
    
    parser.add_argument('--project', '-p', required=True, help='Google Cloud Project ID')
    parser.add_argument('--service', '-s', required=True, help='Path to service account JSON')
    parser.add_argument('--date', '-d', help='Specific date to process (YYYY-MM-DD)')
    parser.add_argument('--start-date', help='Start date for range (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for range (YYYY-MM-DD)')
    parser.add_argument('--force', '-f', action='store_true', 
                       help='Force reprocess even if data exists')
    parser.add_argument('--test', '-t', action='store_true',
                       help='Test mode - limit to 30 records per date')
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
    
    # Initialize scraper
    scraper = DIBBSUnifiedScraper(config, skip_existing=not args.force, test_mode=args.test)
    
    # Determine what to process
    if args.date:
        # Single date
        date_obj = datetime.strptime(args.date, '%Y-%m-%d')
        date_code = date_obj.strftime('%y%m%d')
        result = scraper.process_date(args.date, date_code)
        results = [result]
        
    elif args.start_date and args.end_date:
        # Date range
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        results = scraper.process_date_range(start_date, end_date)
        
    else:
        # Default to today
        today = datetime.now()
        date_str = today.strftime('%Y-%m-%d')
        date_code = today.strftime('%y%m%d')
        result = scraper.process_date(date_str, date_code)
        results = [result]
    
    # Final summary
    print("\n" + "="*80)
    print("PROCESSING COMPLETE")
    print("="*80)
    
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"Total dates processed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if failed > 0:
        print("\nFailed dates:")
        for r in results:
            if not r['success']:
                print(f"  - {r['date']}")
    
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    exit(main())