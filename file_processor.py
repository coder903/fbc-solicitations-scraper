#!/usr/bin/env python3
"""
DIBBS File Processor - Extracts, processes, and uploads DIBBS files to Google Cloud Storage
"""

import argparse
import json
import logging
import os
import zipfile
from datetime import datetime
from typing import Dict, List, Optional
import re

from util.storage_api import Storage
from util.configuration import Configuration

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BUCKET = 'fbc-requests'  # Following the same pattern as fbc-awards
DATASET = 'DIBBS'


class DIBBSFileProcessor:
    def __init__(self, config):
        self.config = config
        self.storage = Storage(config, "service")
        
    def extract_zip_files(self, input_dir: str, extract_dir: str) -> Dict[str, List[str]]:
        """Extract all zip files and return mapping of extracted files"""
        extracted_files = {}
        
        for subdir in ['ca_files', 'bq_files', 'as_files']:
            subdir_path = os.path.join(input_dir, subdir)
            if not os.path.exists(subdir_path):
                continue
                
            for filename in os.listdir(subdir_path):
                if filename.endswith('.zip'):
                    zip_path = os.path.join(subdir_path, filename)
                    extract_subdir = os.path.join(extract_dir, filename[:-4])  # Remove .zip
                    
                    try:
                        os.makedirs(extract_subdir, exist_ok=True)
                        with zipfile.ZipFile(zip_path, 'r') as zf:
                            zf.extractall(extract_subdir)
                            extracted = zf.namelist()
                            extracted_files[filename] = extracted
                            logger.info(f"Extracted {len(extracted)} files from {filename}")
                    except Exception as e:
                        logger.error(f"Error extracting {filename}: {str(e)}")
                        
        return extracted_files
    
    def parse_in_file(self, filepath: str) -> List[Dict]:
        """Parse IN*.txt file to extract metadata"""
        records = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if len(line.strip()) == 0:
                        continue
                        
                    # Parse fixed-width format based on document spec
                    record = {
                        'solicitation_number': line[0:13].strip(),
                        'nsn_part_number': line[13:59].strip(),
                        'purchase_request': line[59:72].strip(),
                        'return_by_date': line[72:80].strip(),
                        'file_name': line[80:99].strip(),
                        'quantity': line[99:106].strip(),
                        'unit_issue': line[106:108].strip(),
                        'nomenclature': line[108:129].strip(),
                        'buyer_code': line[129:134].strip(),
                        'amsc': line[134:135].strip() if len(line) > 134 else '',
                        'item_type': line[135:136].strip() if len(line) > 135 else '',
                        'small_business_setaside': line[136:137].strip() if len(line) > 136 else '',
                        'setaside_percentage': line[137:140].strip() if len(line) > 137 else ''
                    }
                    
                    # Clean up NSN (remove hyphens)
                    if record['nsn_part_number']:
                        record['nsn_clean'] = record['nsn_part_number'].replace('-', '')
                    
                    records.append(record)
                    
        except Exception as e:
            logger.error(f"Error parsing IN file {filepath}: {str(e)}")
            
        return records
    
    def parse_as_file(self, filepath: str) -> List[Dict]:
        """Parse AS*.txt file for approved sources"""
        approved_sources = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
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
                        
        except Exception as e:
            logger.error(f"Error parsing AS file {filepath}: {str(e)}")
            
        return approved_sources
    
    def parse_bq_file(self, filepath: str) -> List[Dict]:
        """Parse BQ*.txt file for batch quote data"""
        batch_quotes = []
        
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
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
                            # Add more fields as needed
                        }
                        
                        # Clean NSN
                        if quote['nsn_part_number']:
                            quote['nsn_clean'] = quote['nsn_part_number'].replace('-', '')
                            
                        batch_quotes.append(quote)
                        
        except Exception as e:
            logger.error(f"Error parsing BQ file {filepath}: {str(e)}")
            
        return batch_quotes
    
    def upload_to_gcs(self, local_path: str, gcs_path: str, mimetype: str = 'application/octet-stream') -> bool:
        """Upload file to Google Cloud Storage"""
        try:
            with open(local_path, 'rb') as f:
                self.storage.object_put(
                    bucket=BUCKET,
                    filename=gcs_path,
                    data=f,
                    mimetype=mimetype
                )
            logger.info(f"Uploaded {local_path} to gs://{BUCKET}/{gcs_path}")
            return True
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {str(e)}")
            return False
    
    def process_date_files(self, date_str: str, input_dir: str, extract_dir: str) -> Dict:
        """Process all files for a specific date"""
        date_code = datetime.strptime(date_str, '%Y-%m-%d').strftime('%y%m%d')
        stats = {
            'files_processed': 0,
            'files_uploaded': 0,
            'in_records': 0,
            'as_records': 0,
            'bq_records': 0,
            'solicitations_found': set()
        }
        
        # Extract zip files
        extracted = self.extract_zip_files(input_dir, extract_dir)
        
        # First pass: parse IN file to get solicitation -> PR mapping
        solicitation_pr_map = {}
        in_records = []
        
        in_file_path = os.path.join(input_dir, 'in_files', f'in{date_code}.txt')
        if os.path.exists(in_file_path):
            in_records = self.parse_in_file(in_file_path)
            stats['in_records'] = len(in_records)
            
            # Build solicitation -> PR mapping
            for record in in_records:
                sol_num = record.get('solicitation_number', '').strip()
                pr_num = record.get('purchase_request', '').strip()
                if sol_num and pr_num:
                    if sol_num not in solicitation_pr_map:
                        solicitation_pr_map[sol_num] = set()
                    solicitation_pr_map[sol_num].add(pr_num)
                    stats['solicitations_found'].add(sol_num)
        
        # Process BQ files to get additional solicitation info
        bq_records = []
        as_records = []
        
        for extract_subdir in os.listdir(extract_dir):
            subdir_path = os.path.join(extract_dir, extract_subdir)
            if not os.path.isdir(subdir_path):
                continue
                
            for filename in os.listdir(subdir_path):
                file_path = os.path.join(subdir_path, filename)
                
                # Process BQ files
                if filename.startswith('bq') and filename.endswith('.txt'):
                    bq_data = self.parse_bq_file(file_path)
                    bq_records.extend(bq_data)
                    stats['bq_records'] += len(bq_data)
                    
                    # Add to solicitation mapping
                    for record in bq_data:
                        sol_num = record.get('solicitation_number', '').strip()
                        pr_num = record.get('purchase_request', '').strip()
                        if sol_num and pr_num:
                            if sol_num not in solicitation_pr_map:
                                solicitation_pr_map[sol_num] = set()
                            solicitation_pr_map[sol_num].add(pr_num)
                            stats['solicitations_found'].add(sol_num)
                
                # Process AS files
                elif filename.startswith('as') and filename.endswith('.txt'):
                    as_data = self.parse_as_file(file_path)
                    as_records.extend(as_data)
                    stats['as_records'] += len(as_data)
        
        # Now organize and upload files by solicitation structure
        for sol_num, pr_numbers in solicitation_pr_map.items():
            # Upload IN file to each PR folder
            if os.path.exists(in_file_path):
                for pr_num in pr_numbers:
                    gcs_path = f"{DATASET}/{sol_num}/{pr_num}/in{date_code}.txt"
                    if self.upload_to_gcs(in_file_path, gcs_path, 'text/plain'):
                        stats['files_uploaded'] += 1
            
            # Upload BQ and AS files from extracted directories
            for extract_subdir in os.listdir(extract_dir):
                subdir_path = os.path.join(extract_dir, extract_subdir)
                if not os.path.isdir(subdir_path):
                    continue
                    
                # Upload BQ file to each PR folder
                bq_file = os.path.join(subdir_path, f'bq{date_code}.txt')
                if os.path.exists(bq_file):
                    for pr_num in pr_numbers:
                        gcs_path = f"{DATASET}/{sol_num}/{pr_num}/bq{date_code}.txt"
                        if self.upload_to_gcs(bq_file, gcs_path, 'text/plain'):
                            stats['files_uploaded'] += 1
                
                # Upload AS file to each PR folder
                as_file = os.path.join(subdir_path, f'as{date_code}.txt')
                if os.path.exists(as_file):
                    for pr_num in pr_numbers:
                        gcs_path = f"{DATASET}/{sol_num}/{pr_num}/as{date_code}.txt"
                        if self.upload_to_gcs(as_file, gcs_path, 'text/plain'):
                            stats['files_uploaded'] += 1
        
        # Process PDFs and organize by solicitation
        for extract_subdir in os.listdir(extract_dir):
            subdir_path = os.path.join(extract_dir, extract_subdir)
            if not os.path.isdir(subdir_path):
                continue
                
            for filename in os.listdir(subdir_path):
                if filename.endswith('.pdf') or filename.endswith('.PDF'):
                    file_path = os.path.join(subdir_path, filename)
                    stats['files_processed'] += 1
                    
                    # Extract solicitation number from filename
                    match = re.match(r'(SPE\w+\d+)', filename, re.IGNORECASE)
                    if match:
                        solicitation = match.group(1).upper()
                        gcs_path = f"{DATASET}/{solicitation}/{filename}"
                        if self.upload_to_gcs(file_path, gcs_path, 'application/pdf'):
                            stats['files_uploaded'] += 1
        
        # Save parsed data as JSON for BigQuery loading
        parsed_data_dir = os.path.join(extract_dir, 'parsed_data')
        os.makedirs(parsed_data_dir, exist_ok=True)
        
        # Save all parsed data
        with open(os.path.join(parsed_data_dir, f'in{date_code}_parsed.json'), 'w') as f:
            json.dump(in_records, f, indent=2)
        
        with open(os.path.join(parsed_data_dir, f'bq{date_code}_parsed.json'), 'w') as f:
            json.dump(bq_records, f, indent=2)
            
        with open(os.path.join(parsed_data_dir, f'as{date_code}_parsed.json'), 'w') as f:
            json.dump(as_records, f, indent=2)
        
        stats['solicitations_found'] = len(stats['solicitations_found'])
        return stats


def main():
    parser = argparse.ArgumentParser(description='DIBBS File Processor and GCS Uploader')
    parser.add_argument('--project', '-p', required=True, help='Google Cloud Project ID')
    parser.add_argument('--service', '-s', required=True, help='Path to service account JSON')
    parser.add_argument('--input-dir', '-i', default='./dibbs_data', help='Input directory with downloaded files')
    parser.add_argument('--extract-dir', '-e', default='./dibbs_extracted', help='Directory for extracted files')
    parser.add_argument('--date', '-d', required=True, help='Date to process (YYYY-MM-DD)')
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
    processor = DIBBSFileProcessor(config)
    
    # Process files
    logger.info(f"Processing files for {args.date}")
    stats = processor.process_date_files(args.date, args.input_dir, args.extract_dir)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Processing Summary for {args.date}")
    print(f"{'='*60}")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Files uploaded to GCS: {stats['files_uploaded']}")
    print(f"Solicitations found: {stats['solicitations_found']}")
    print(f"IN records parsed: {stats['in_records']}")
    print(f"AS records parsed: {stats['as_records']}")
    print(f"BQ records parsed: {stats['bq_records']}")
    print(f"\nFiles organized in gs://{BUCKET}/DIBBS/<solicitation>/<pr>/...")
    
    return 0


if __name__ == '__main__':
    exit(main())