import argparse
import textwrap
import requests
import json
import time
import ssl
import io
import re
from datetime import date, datetime

from html.parser import HTMLParser

# Using the same utilities as dibbs.py
from util.storage_api import Storage
from util.bigquery_api import BigQuery
from util.csv import column_header_sanitize
from util.configuration import Configuration

BUCKET = 'fbc-solicitations'  # Different bucket for solicitations
DATASET = 'DIBBS'
DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'

# Same headers as dibbs.py
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
    """
    Reusing the FormParser from dibbs.py
    """
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


class RfqRecsParser(HTMLParser):
    """
    Custom parser for RFQ/Solicitations table data
    """
    @staticmethod
    def parse(page):
        parser = RfqRecsParser()
        parser.feed(page)
        parser.close()
        return parser.records, parser.get_data()

    def __init__(self):
        super().__init__()
        self.records = 0
        self.rows = []
        self.row = {}
        self.inside_tr = False
        self.inside_td = False
        self.td_count = 0
        self.inside_count = False
        self.current_data = []
        self.inside_a = False
        self.current_link_href = None
        self.current_link_text = []
        self.td_content = []  # Store all content for current TD
        self.collecting_tech_docs = False
        self.current_tech_doc = {}
        self.inside_img = False

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == 'tr':
            # Check for table rows with class BgWhite or BgSilver
            if attrs_dict.get('class') in ['BgWhite', 'BgSilver']:
                self.inside_tr = True
                self.td_count = 0
                self.row = {
                    'item_number': '',
                    'nsn_part_number': '',
                    'nomenclature': '',
                    'technical_documents': [],
                    'solicitation': '',
                    'solicitation_url': '',
                    'setaside_type': '',
                    'rfq_quote_status': '',
                    'purchase_request': '',
                    'pr_number': '',
                    'quantity': '',
                    'issued': '',
                    'return_by': ''
                }
        elif tag == 'td' and self.inside_tr:
            self.inside_td = True
            self.td_count += 1
            self.td_content = []
            self.current_data = []
        elif tag == 'span' and attrs_dict.get('id') == 'ctl00_cph1_lblRecCount':
            self.inside_count = True
        elif tag == 'a' and self.inside_td:
            self.inside_a = True
            self.current_link_href = attrs_dict.get('href', '')
            self.current_link_text = []
            
            # Technical Documents column (column 4)
            if self.td_count == 4 and 'title' in attrs_dict:
                self.current_tech_doc = {
                    'title': attrs_dict.get('title', ''),
                    'url': self.current_link_href
                }
                self.collecting_tech_docs = True
            
            # Solicitation column (column 5)
            elif self.td_count == 5:
                # Capture the first link in the solicitation column
                if not self.row['solicitation_url']:  # Only set if not already set
                    self.row['solicitation_url'] = self.current_link_href
        
        elif tag == 'img' and self.inside_td:
            # Check for set-aside type images in solicitation column
            if self.td_count == 5 and 'alt' in attrs_dict:
                alt_text = attrs_dict.get('alt', '').strip()
                if alt_text:
                    self.row['setaside_type'] = alt_text

    def handle_endtag(self, tag):
        if tag == 'tr' and self.inside_tr:
            self.inside_tr = False
            if any(self.row.values()):  # Only add row if it has data
                self.rows.append(self.row)
        elif tag == 'td' and self.inside_td:
            self.inside_td = False
            
            # Join all collected text for this cell
            cell_text = ' '.join(self.td_content).strip()
            
            # Map to appropriate field based on column number
            if self.td_count == 1:  # Item number
                self.row['item_number'] = cell_text
            elif self.td_count == 2:  # NSN/Part Number
                self.row['nsn_part_number'] = cell_text.replace('-', '')
            elif self.td_count == 3:  # Nomenclature
                self.row['nomenclature'] = cell_text
            elif self.td_count == 4:  # Technical Documents
                # Already handled in handle_starttag
                pass
            elif self.td_count == 5:  # Solicitation
                # Extract solicitation number from text
                if cell_text:
                    # Remove "» Package View" and any other suffixes
                    solicitation_text = cell_text.split('»')[0].strip()
                    # Also remove any unicode arrow characters
                    solicitation_text = solicitation_text.split('\u00bb')[0].strip()
                    self.row['solicitation'] = solicitation_text.replace('-', '')
            elif self.td_count == 6:  # RFQ/Quote Status
                self.row['rfq_quote_status'] = cell_text
            elif self.td_count == 7:  # Purchase Request
                self.row['purchase_request'] = cell_text
                # Parse PR number and quantity
                lines = [line.strip() for line in cell_text.split('\n') if line.strip()]
                if lines:
                    # First line is usually the PR number
                    pr_line = lines[0]
                    # Check if QTY is on the same line as PR number
                    if 'QTY:' in pr_line.upper():
                        # Split PR number and quantity
                        parts = pr_line.upper().split('QTY:')
                        self.row['pr_number'] = parts[0].strip()
                        self.row['quantity'] = parts[1].strip()
                    else:
                        # PR number is the whole first line
                        self.row['pr_number'] = pr_line
                        # Look for quantity in subsequent lines
                        for line in lines[1:]:
                            if 'QTY:' in line.upper():
                                qty_text = line.upper().split('QTY:')[1].strip()
                                self.row['quantity'] = qty_text
                                break
                            elif any(char.isdigit() for char in line):
                                # If no QTY: prefix but contains numbers, might be quantity
                                self.row['quantity'] = line
                                break
            elif self.td_count == 8:  # Issued
                # Convert MM-DD-YYYY to YYYY-MM-DD for BigQuery
                if cell_text and '-' in cell_text:
                    try:
                        date_obj = datetime.strptime(cell_text, '%m-%d-%Y')
                        self.row['issued'] = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        # If parsing fails, keep original
                        self.row['issued'] = cell_text
                else:
                    self.row['issued'] = cell_text
            elif self.td_count == 9:  # Return By
                # Convert MM-DD-YYYY to YYYY-MM-DD for BigQuery
                if cell_text and '-' in cell_text:
                    try:
                        date_obj = datetime.strptime(cell_text, '%m-%d-%Y')
                        self.row['return_by'] = date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        # If parsing fails, keep original
                        self.row['return_by'] = cell_text
                else:
                    self.row['return_by'] = cell_text
        
        elif tag == 'a' and self.inside_a:
            self.inside_a = False
            
            # Finalize technical document
            if self.collecting_tech_docs and self.current_tech_doc:
                if self.current_tech_doc.get('url'):
                    self.row['technical_documents'].append(self.current_tech_doc)
                self.collecting_tech_docs = False
                self.current_tech_doc = {}
            
            # Add link text to td_content
            if self.current_link_text:
                link_text = ''.join(self.current_link_text).strip()
                if link_text:
                    self.td_content.append(link_text)
        
        elif tag == 'span' and self.inside_count:
            self.inside_count = False

    def handle_data(self, data):
        data = data.strip()
        if not data:
            return
            
        if self.inside_count:
            try:
                # Extract number from text like "Records: 123"
                match = re.search(r'\d+', data)
                if match:
                    self.records = int(match.group())
            except ValueError:
                pass
        elif self.inside_a:
            self.current_link_text.append(data)
        elif self.inside_td:
            self.td_content.append(data)

    def get_data(self):
        return self.rows


def dibbs_session(host, verify=True):
    '''
    Reusing the session creation from dibbs.py
    '''
    session = requests.Session()

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

    return session


def dibbs_solicitations(config, day, test_mode=False, max_records=None):
    '''
    Scrape solicitations data for a given day
    '''
    number = 1
    rows = []

    # Activate dibbs access
    session_dibbs = dibbs_session(DIBBS_HOST)

    print(f'Scraping solicitations for {day}')
    if test_mode and max_records:
        print(f'TEST MODE: Limiting to {max_records} records')
    print('PAGE', number)

    # Load first page
    form_action = f'{DIBBS_HOST}RFQ/RfqRecs.aspx?category=post&TypeSrch=dt&Value={day}'
    page = session_dibbs.request('GET', form_action).content.decode('utf-8')
    
    # Debug: save first page for inspection
    if test_mode:
        with open('debug_page1.html', 'w', encoding='utf-8') as f:
            f.write(page)
        print("DEBUG: Saved first page to debug_page1.html")
    
    form_data, _, form_method = FormParser().parse(page)
    count, rows_more = RfqRecsParser().parse(page)
    rows.extend(rows_more)

    print(f"DEBUG: Page 1 - Found {len(rows_more)} records, total so far: {len(rows)}")

    # Check if we've reached the test limit
    if test_mode and max_records and len(rows) >= max_records:
        print(f'TEST MODE: Stopping at {len(rows)} records')
        return rows[:max_records]

    # Loop additional pages
    while len(rows) < count and len(rows_more) > 0:
        # Check test limit before fetching next page
        if test_mode and max_records and len(rows) >= max_records:
            print(f'TEST MODE: Stopping at {len(rows)} records')
            return rows[:max_records]
            
        number += 1
        print('PAGE', number, '(', len(rows), 'of', count, ')')

        form_data['__EVENTTARGET'] = 'ctl00$cph1$grdRfqSearch'  # Note: different grid name for RFQ
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
        
        print(f"DEBUG: Page {number} - Found {len(rows_more)} records, total so far: {len(rows)}")

    return rows


def dibbs_solicitations_storage(config, rows):
    """
    Download solicitation PDFs to storage - reentrant (skips existing files)
    Only downloads main solicitation PDFs, not technical documents
    """
    from util.storage_api import Storage
    import requests
    import time
    
    storage = Storage(config, "service")
    session = dibbs_session(DIBBS_HOST)
    
    stats = {'downloaded': 0, 'skipped': 0, 'failed': 0}
    
    print(f"\nProcessing solicitation PDFs for {len(rows)} solicitations...")
    
    for row in rows:
        # Only download main solicitation document
        if 'solicitation_url' in row and row['solicitation_url']:
            filename = f"{DATASET}/solicitations/{row.get('solicitation', 'unknown')}_solicitation.pdf"
            
            if storage.object_exists(bucket=BUCKET, filename=filename):
                print(f"  Skipping (exists): {filename}")
                stats['skipped'] += 1
            else:
                try:
                    print(f"  Downloading: {row['solicitation_url']} -> {BUCKET}/{filename}")
                    response = session.get(row['solicitation_url'], timeout=60)
                    response.raise_for_status()
                    
                    storage.object_put(
                        bucket=BUCKET,
                        filename=filename,
                        data=io.BytesIO(response.content),
                        mimetype='application/pdf'
                    )
                    stats['downloaded'] += 1
                    time.sleep(0.5)  # Be polite to the server
                except Exception as e:
                    print(f"  Failed: {row['solicitation_url']} - {str(e)}")
                    stats['failed'] += 1
    
    print(f"\nPDF Download Summary: {stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed")
    return stats


def save_json_output(rows, day):
    """
    Save the scraped data to a JSON file
    """
    filename = f"solicitations_{day.replace('-', '_')}.json"
    with open(filename, 'w') as f:
        json.dump(rows, f, indent=2)
    print(f"\nJSON data saved to: {filename}")
    return filename


def print_sample_structure(rows):
    """
    Print a sample of the data structure for inspection
    """
    if not rows:
        print("No data to display")
        return
        
    print("\n=== SAMPLE RECORD STRUCTURE ===")
    print(json.dumps(rows[0], indent=2))
    
    print("\n=== FIRST 3 RECORDS ===")
    print(json.dumps(rows[:3], indent=2))
    
    # Show statistics
    print("\n=== DATA STATISTICS ===")
    print(f"Total records: {len(rows)}")
    
    # Count records with various fields
    tech_docs_count = sum(1 for r in rows if 'technical_documents' in r and r['technical_documents'])
    setaside_count = sum(1 for r in rows if 'setaside_type' in r and r['setaside_type'])
    
    print(f"Records with technical documents: {tech_docs_count}")
    print(f"Records with set-aside type: {setaside_count}")
    
    # Show unique set-aside types
    setaside_types = set(r.get('setaside_type', '') for r in rows if r.get('setaside_type'))
    print(f"\nUnique set-aside types found: {len(setaside_types)}")
    for st in sorted(setaside_types)[:10]:  # Show first 10
        if st:  # Only show non-empty values
            print(f"  - {st}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
        DIBBS Solicitations Scraper
        
        This script scrapes solicitation/RFQ data from DIBBS for a given date.
        
        Example:
          python dibbs_solicitations.py -s [service.json] -p [project] --day 06-03-2025
          python dibbs_solicitations.py -s [service.json] -p [project] --test
    """))

    parser.add_argument('--project', '-p', help='Cloud ID of Google Cloud Project.', default=None)
    parser.add_argument('--service', '-s', help='Path to SERVICE credentials json file.', default=None)
    parser.add_argument('--verbose', '-v', help='Print all the steps as they happen.', action='store_true')
    parser.add_argument('--day', '-d', help='Date to load in MM-DD-YYYY format.', default=date.today().strftime("%m-%d-%Y"))
    parser.add_argument('--test', '-t', help='Run test mode.', action='store_true')
    parser.add_argument('--output', '-o', help='Output JSON filename (default: solicitations_MM_DD_YYYY.json)', default=None)

    args = parser.parse_args()

    config = Configuration(
        project=args.project,
        service=args.service,
        verbose=args.verbose
    )

    if args.test:
        # Test with a specific date
        test_day = '06-03-2025'
        rows = dibbs_solicitations(config, test_day, test_mode=True, max_records=30)
        
        print(f"\nTotal records scraped: {len(rows)}")
        
        # Print sample structure
        print_sample_structure(rows)
        
        # Save to file
        filename = args.output or f"solicitations_test_{test_day.replace('-', '_')}.json"
        with open(filename, 'w') as f:
            json.dump(rows, f, indent=2)
        print(f"\nFull JSON data saved to: {filename}")
        
        # Show what files would be downloaded
        print("\n=== SAMPLE FILE DOWNLOADS (first 5) ===")
        dibbs_solicitations_storage(config, rows[:5])
    else:
        rows = dibbs_solicitations(config, args.day)
        
        print(f"\nTotal records scraped: {len(rows)}")
        
        # Print sample structure
        print_sample_structure(rows)
        
        # Save to file
        if args.output:
            filename = args.output
        else:
            filename = f"solicitations_{args.day.replace('-', '_')}.json"
        
        with open(filename, 'w') as f:
            json.dump(rows, f, indent=2)
        print(f"\nFull JSON data saved to: {filename}")
        
        # Placeholder for storage operations
        print("\n=== FILE DOWNLOAD SUMMARY ===")
        dibbs_solicitations_storage(config, rows)