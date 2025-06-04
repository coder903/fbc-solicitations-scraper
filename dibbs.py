import argparse
import textwrap
import requests
import json
import time
import ssl
import io

from datetime import date
from html.parser import HTMLParser

from util.storage_api import Storage
from util.bigquery_api import BigQuery
from util.csv import column_header_sanitize
from util.configuration import Configuration

BUCKET = 'fbc-awards'
DATASET = 'DIBBS'
DIBBS_HOST = 'https://www.dibbs.bsm.dla.mil/'
DIBBS2_HOST = 'https://dibbs2.bsm.dla.mil/'


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
  A custom HTMLParser class to extract form data, action, and method.
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
    self.form_method = 'get' # Default method is GET
    self.parsing_form = False # Flag to indicate if we are inside a form

  def handle_starttag(self, tag, attrs):
    """
    Handles the start tags of HTML elements. This method is called when the parser
    encounters a start tag (e.g., <form>, <input>).
    """
    if tag == 'form':
      self.parsing_form = True
      attrs = dict(attrs)
      self.form_action = attrs['action'] 
      self.form_method = attrs['method'].upper() 
      self.form_data = {}
    elif self.parsing_form and tag == 'input':
      attrs = dict(attrs)
      if 'name' in attrs  and 'value' in attrs:
        self.form_data[attrs['name']] = attrs['value']


  def handle_endtag(self, tag):
    """Handles the end tags of HTML elements."""
    if tag == 'form':
      self.parsing_form = False # Exit form parsing mode

  def get_data(self):
    """Returns the extracted form data, action, and method."""
    return self.form_data, self.form_action, self.form_method


class AwdRecsParser(HTMLParser):
  """
  A custom HTMLParser to extract data from <td> tags within <tr> tags of class "AwdRecs".
  """

  @staticmethod
  def parse(page):
    parser = AwdRecsParser()
    parser.feed(page)
    parser.close()
    return parser.records, parser.get_data()

  def __init__(self):
    super().__init__()
    self.records = 0
    self.rows = []
    self.row = {}
    self.inside_tr = False
    self.inside_td = None # stores id
    self.inside_span = False
    self.inside_count = False

  def handle_starttag(self, tag, attrs):
    """
    Handles the start tags of HTML elements.
    """
    if tag == 'tr':
      if ('class', 'BgWhite') in attrs or ('class', 'BgSilver') in attrs:
        self.inside_tr = True
        self.row = { 'Links':{} }
    elif tag == 'span':
      if self.inside_td:
         self.inside_span = True 
      elif  self.inside_tr:
        attrs = dict(attrs)
        if 'id' in attrs:
          self.inside_td = dict(attrs)['id'].split('_lbl', 1)[-1]
      elif ('id', 'ctl00_cph1_lblRecCount') in attrs:
        self.inside_count = True
    elif self.inside_td and tag == 'a' and self.inside_td != 'Cage':
      attrs = dict(attrs)
      self.row['Links'][column_header_sanitize(attrs['title']).replace('Link_To_', '')] = attrs['href']
    

  def handle_endtag(self, tag):
    """
    Handles the end tags of HTML elements.
    """
    if tag == 'tr' and self.inside_tr:
      self.inside_tr = False
      if self.row:
        self.row.setdefault('DeliveryOrder', '')
        self.rows.append(self.row)
    elif tag == 'span':
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
      if self.inside_td == 'TotalContactPrice':
        data = data.replace('$', '').replace(',', '')
      self.row.setdefault(self.inside_td, '')
      self.row[self.inside_td] += data
    elif self.inside_count:
      try:
        self.records = int(data.strip()) # loads twice (span + bold), second time in strong (exploit side effect)
      except ValueError:
        pass

  def get_data(self):
    """Returns the extracted data."""
    return self.rows


#def get_imported_records(config, day):
#  """
#  This is not used, the whole table is replaced every day it is run to ensure consistency.
#  """
#  table = f"CLEAN_{day.replace('-', '_')}"
#  if BigQuery(config, 'service').table_exists(
#    project_id = config.project, 
#    dataset_id = DATASET, 
#    table_id = table
#  ):
#    return set(tuple(pair) for pair in BigQuery(config, 'service').query_to_rows(
#      project_id = config.project,
#      dataset_id = DATASET,
#      query = f"SELECT AwardBasicNumber, Nsn FROM `{table}`",
#      as_object=False
#    ))
#  else:
#    return set()


def get_request_headers(headers=None):
  """
  Combines the default headers with any additional headers provided for a request.
  """
  req_headers = HEADERS.copy() # Start with the default headers
  if headers:
    req_headers.update(headers)  # Update with any specific headers
  return req_headers


def inspect_cookies(session):
  """
  Prints information about cookies for debug.
  """
  for cookie in session.cookies:
    print(f"Cookie: {cookie.name}, Domain: {cookie.domain}")


def dibbs_session(host, verify=True):
  '''
  The first page of dibbs sets a cookie which is triggered via a form submit.
  This is required to access all other pages.

  The steps are:
    - Pass believable headers.
    - Load the start page (with OK form).
    - Parse form out of page.
    - Submit form to set cookie.
    - Store cookies in Session class which has get/post wrappers.
    - Return session for future use.
  '''

  session = requests.Session()

  # load first page
  page = session.request('GET', url = host + "dodwarning.aspx?goto=/", verify=verify).content.decode('utf-8')

  # parse form (form must match all parameters exactly on page)
  form_data, form_action, form_method = FormParser().parse(page)

  # submit form to set cookie (response doesnt matter)
  page = session.request('POST', 
    url = host + form_action,
    data = form_data,
    verify=verify
  )

  inspect_cookies(session)

  return session

def dibbs_awards_table(day):
  month, day, year = day.split('-')
  return f"RAW_{year}_{month}_{day}"

def dibbs_awards_save(config, day, rows):
  """
  Write the daily records to BigQuery.
  """

  table = dibbs_awards_table(day)
  BigQuery(config, 'service').json_to_table(
    project_id=config.project,
    dataset_id=DATASET,
    table_id=table,
    json_data=rows,
    schema=None
  )

  BigQuery(config, 'service').query_to_table(
    project_id=config.project,
    dataset_id=DATASET,
    table_id=table.replace('RAW', 'CLEAN'),
    query = f'''
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
        
      FROM `DIBBS.{table}` 
      GROUP BY 1,2,3,4,5,6,7,8
    ''',
    disposition='WRITE_TRUNCATE'
  ) 


def dibbs_awards_storage(config, rows):
  """
  Session against content server and donwload files to storage.
  """
  done = set()
  session = dibbs_session(DIBBS2_HOST, verify=False)
  for row in rows:
    key = (row['AwardBasicNumber'], row['DeliveryOrder']) 
    if key not in done:
      done.add(key)
      for title, url in row['Links'].items():
        if url.endswith('.PDF'):
          filename = f"{DATASET}/{row['AwardBasicNumber']}_{row['DeliveryOrder']}_{title}.pdf"
          if Storage(config, 'service').object_exists(
            bucket = BUCKET,
            filename = filename
          ):
            print('EXISTS:', BUCKET, filename)
          else: 
            print('UPLOAD:', BUCKET, filename)
            data = session.request('GET', verify=False,
              url=url,
              headers = {
                'Host': 'dibbs2.bsm.dla.mil',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
              }
            ).content
            Storage(config, 'service').object_put(
              bucket = BUCKET,
              filename = filename,
              data = io.BytesIO(data),
              mimetype='application/pdf'
            )


def dibbs_awards(config, day):
  '''
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
  '''

  number = 1
  rows = []

  # activate dibbs access
  session_dibbs = dibbs_session(DIBBS_HOST)

  print('PAGE', number)

  # load first page
  form_action = f'{DIBBS_HOST}Awards/AwdRecs.aspx?Category=post&TypeSrch=cq&Value={day}'
  page = session_dibbs.request('GET', form_action).content.decode('utf-8')
  form_data, _, form_method = FormParser().parse(page)
  count, rows_more = AwdRecsParser().parse(page)
  rows.extend(rows_more) 

  # loop additional ages
  while len(rows) < count and len(rows_more) > 0:

    number += 1
    print('PAGE', number, '(', len(rows), 'of', count, ')')

    form_data['__EVENTTARGET'] = 'ctl00$cph1$grdAwardSearch'
    form_data['__EVENTARGUMENT'] = f'Page${number}'

    # default select fields
    form_data['ctl00$ddlNavigation'] = '' 
    form_data['ctl00$ddlGotoDB'] = '' 
    form_data['ctl00$txtValue'] = ''

    # remove buttons
    del form_data["ctl00$butNavigation"]
    del form_data["ctl00$butDbSearch"]

    page = session_dibbs.request('POST',
      url = form_action,
      data = form_data,
    ).content.decode('utf-8')

    form_data, _, form_method = FormParser().parse(page)
    _, rows_more = AwdRecsParser().parse(page)
    rows.extend(rows_more) 

  # save all rows and PDFs
  dibbs_awards_save(config, day, rows)
  dibbs_awards_storage(config, rows)


if __name__ == '__main__':

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
    Use this to download dibbs awards (and rfqs in future) to storage and BigQuery.
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
      python dibbs.py -s [service.json file path] -p [gcp project]-surf-426917-k8 --day [MM-DD-YYYY to download]
      python dibbs.py -s [service.json file path] -p [gcp project]-surf-426917-k8 --test
  """))

  parser.add_argument('--project', '-p', help='Cloud ID of Google Cloud Project.', default=None)
  parser.add_argument('--service', '-s', help='Path to SERVICE credentials json file.', default=None)
  parser.add_argument('--verbose', '-v', help='Print all the steps as they happen.', action='store_true')

  parser.add_argument('--day', '-d', help='Date to load in MM-DD-YYYY format.', default=date.today().strftime("%m-%d-%Y")) 
  parser.add_argument('--test', '-t', help='Run test (requires CAGE.ZIP).', action='store_true')

  args = parser.parse_args()

  config = Configuration(
    project=args.project,
    service=args.service,
    verbose=args.verbose
  )

  if args.test:
    with open('AwdRecs.aspx', 'r') as file:
      count, rows = AwdRecsParser.parse(file.read())
      print('COUNT:', count)
      print('RECORDS:', json.dumps(rows, indent=2))
      dibbs_awards_save(config, 'TEST', rows)
      dibbs_awards_storage(config, rows)

  else:
    dibbs_awards(config, args.day)