#!/usr/bin/env python3
"""
DIBBS Historical Data BigQuery Loader - Processes crawled data and loads to BigQuery
"""

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from util.bigquery_api import BigQuery
from util.configuration import Configuration

# Import the enhanced loader
from dibbs_bigquery_loader_enhanced import DIBBSBigQueryLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATASET = 'DIBBS'


class DIBBSHistoricalLoader:
    def __init__(self, config, temp_dir='./dibbs_historical_temp'):
        self.config = config
        self.temp_dir = temp_dir
        self.loader = DIBBSBigQueryLoader(config)
        self.bq = BigQuery(config, "service")
        
    def load_crawl_results(self) -> Dict:
        """Load the crawl results file"""
        results_file = os.path.join(self.temp_dir, 'crawl_results.json')
        
        if not os.path.exists(results_file):
            raise FileNotFoundError(f"Crawl results not found at {results_file}")
            
        with open(results_file, 'r') as f:
            return json.load(f)
    
    def parse_date_files(self, date_code: str) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """Parse all files for a specific date"""
        date_dir = os.path.join(self.temp_dir, date_code)
        
        # Load web scrape data
        web_data = []
        sol_file = os.path.join(date_dir, f'solicitations_{date_code}.json')
        if os.path.exists(sol_file):
            with open(sol_file, 'r') as f:
                web_data = json.load(f)
        
        # Parse IN file
        in_data = []
        in_file = os.path.join(date_dir, f'in{date_code}.txt')
        if os.path.exists(in_file):
            from dibbs_file_processor import DIBBSFileProcessor
            processor = DIBBSFileProcessor(self.config)
            in_data = processor.parse_in_file(in_file)
        
        # Parse BQ file
        bq_data = []
        bq_file = os.path.join(date_dir, f'bq{date_code}.txt')
        if os.path.exists(bq_file):
            from dibbs_file_processor import DIBBSFileProcessor
            processor = DIBBSFileProcessor(self.config)
            bq_data = processor.parse_bq_file(bq_file)
        
        # Parse AS file
        as_data = []
        as_file = os.path.join(date_dir, f'as{date_code}.txt')
        if os.path.exists(as_file):
            from dibbs_file_processor import DIBBSFileProcessor
            processor = DIBBSFileProcessor(self.config)
            as_data = processor.parse_as_file(as_file)
        
        return web_data, in_data, bq_data, as_data
    
    def process_historical_data(self, mode='merge'):
        """Process all crawled data and load to BigQuery"""
        
        # Load crawl results
        crawl_data = self.load_crawl_results()
        results = crawl_data.get('results', [])
        
        logger.info(f"Processing {len(results)} dates from crawl results")
        
        success_count = 0
        error_count = 0
        
        for result in results:
            if not result.get('success'):
                logger.warning(f"Skipping {result['date']} - crawl was not successful")
                continue
                
            date_str = result['date']
            date_obj = datetime.strptime(date_str, '%m-%d-%Y')
            date_formatted = date_obj.strftime('%Y-%m-%d')
            date_code = date_obj.strftime('%y%m%d')
            
            logger.info(f"\nProcessing {date_str} for BigQuery...")
            
            try:
                # Parse all files
                web_data, in_data, bq_data, as_data = self.parse_date_files(date_code)
                
                logger.info(f"  Web scrape records: {len(web_data)}")
                logger.info(f"  IN file records: {len(in_data)}")
                logger.info(f"  BQ file records: {len(bq_data)}")
                logger.info(f"  AS file records: {len(as_data)}")
                
                # Merge data sources
                merged_data = self.loader.merge_data_sources(web_data, in_data, bq_data, as_data)
                
                if merged_data:
                    # Load to BigQuery
                    if self.loader.load_to_bigquery(merged_data, date_formatted, mode=mode):
                        success_count += 1
                        logger.info(f"  ✓ Successfully loaded {len(merged_data)} solicitations")
                    else:
                        error_count += 1
                        logger.error(f"  ✗ Failed to load to BigQuery")
                else:
                    logger.warning(f"  No data to load for {date_str}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"  ✗ Error processing {date_str}: {str(e)}")
        
        # Summary
        print("\n" + "=" * 80)
        print("BIGQUERY LOAD COMPLETE")
        print("=" * 80)
        print(f"Dates processed successfully: {success_count}")
        print(f"Dates with errors: {error_count}")
        print(f"Mode: {mode}")
        
        if mode != 'daily_only':
            print(f"\nMaster table: {DATASET}.SOLICITATIONS_MASTER")
            
            # Get master table stats
            try:
                query = f"""
                SELECT 
                    COUNT(DISTINCT solicitation_number) as total_solicitations,
                    COUNT(*) as total_records,
                    MIN(DATE(scrape_timestamp)) as earliest_date,
                    MAX(DATE(scrape_timestamp)) as latest_date
                FROM `{self.config.project}.{DATASET}.SOLICITATIONS_MASTER`
                """
                
                stats = self.bq.query(query)
                if stats:
                    print(f"Total unique solicitations: {stats[0]['total_solicitations']:,}")
                    print(f"Date range: {stats[0]['earliest_date']} to {stats[0]['latest_date']}")
                    
            except Exception as e:
                logger.debug(f"Could not get master table stats: {str(e)}")
    
    def create_historical_views(self):
        """Create useful views for the historical data"""
        
        views = [
            {
                'name': 'SOLICITATIONS_LATEST',
                'description': 'Latest version of each solicitation',
                'query': f"""
                CREATE OR REPLACE VIEW `{self.config.project}.{DATASET}.SOLICITATIONS_LATEST` AS
                SELECT * EXCEPT(row_num)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY solicitation_number ORDER BY last_updated DESC) as row_num
                    FROM `{self.config.project}.{DATASET}.SOLICITATIONS_MASTER`
                )
                WHERE row_num = 1
                """
            },
            {
                'name': 'SOLICITATIONS_WITH_SOURCES',
                'description': 'Solicitations with approved sources flattened',
                'query': f"""
                CREATE OR REPLACE VIEW `{self.config.project}.{DATASET}.SOLICITATIONS_WITH_SOURCES` AS
                SELECT 
                    s.solicitation_number,
                    s.return_by_date,
                    s.small_business_setaside,
                    s.setaside_type,
                    c.nsn,
                    c.part_number,
                    c.nomenclature,
                    c.quantity,
                    c.unit_of_issue,
                    c.purchase_request,
                    a.supplier_cage_code,
                    a.supplier_part_number,
                    a.supplier_name
                FROM `{self.config.project}.{DATASET}.SOLICITATIONS_LATEST` s,
                UNNEST(clins) as c
                LEFT JOIN UNNEST(c.approved_sources) as a
                """
            }
        ]
        
        logger.info("\nCreating BigQuery views...")
        
        for view in views:
            try:
                self.bq.query(view['query'])
                logger.info(f"  ✓ Created view: {view['name']}")
            except Exception as e:
                logger.error(f"  ✗ Error creating view {view['name']}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description='DIBBS Historical Data BigQuery Loader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script processes the data crawled by dibbs_historical_crawler.py and loads it into BigQuery.

Examples:
  # Load all crawled data with merge mode (default)
  python dibbs_historical_loader.py -p PROJECT_ID -s service.json
  
  # Load with daily tables only
  python dibbs_historical_loader.py -p PROJECT_ID -s service.json --mode daily_only
  
  # Create views after loading
  python dibbs_historical_loader.py -p PROJECT_ID -s service.json --create-views
        """
    )
    
    parser.add_argument('--project', '-p', required=True, help='Google Cloud Project ID')
    parser.add_argument('--service', '-s', required=True, help='Path to service account JSON')
    parser.add_argument('--temp-dir', default='./dibbs_historical_temp', 
                       help='Directory with crawled data')
    parser.add_argument('--mode', '-m', choices=['daily_only', 'append', 'merge'], default='merge',
                       help='Load mode: daily_only, append, or merge (default)')
    parser.add_argument('--create-views', action='store_true',
                       help='Create BigQuery views after loading')
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
    
    # Initialize loader
    loader = DIBBSHistoricalLoader(config, args.temp_dir)
    
    # Process historical data
    loader.process_historical_data(mode=args.mode)
    
    # Create views if requested
    if args.create_views:
        loader.create_historical_views()
    
    return 0


if __name__ == '__main__':
    exit(main())