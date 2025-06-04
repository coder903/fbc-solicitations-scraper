###########################################################################
#
#  Copyright 2020 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

import os
import errno
import json
import httplib2
import mimetypes
from time import sleep
from io import BytesIO

from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

from util.misc import memory_scale
from util.google_api import API_Storage
from util.csv import find_utf8_split


STORAGE_CHUNKSIZE = memory_scale(maximum=200 * 1024**3, multiple=256 * 1024)
RETRIES = 3



def makedirs_safe(path):
  try:
    os.makedirs(path)
  except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(path):
      pass
    else:
      raise


def parse_path(path):
  try:
    return path.rsplit('/', 1)[0]
  except:
    return ''


def parse_filename(path, url=False):
  f = path
  try:
    if url:
      f = f.split('?', 1)[0]
    f = f.rsplit('/', 1)[1]
  except:
    pass
  return f


class Storage():

  def __init__(self, config, auth):
    self.config = config
    self.auth = auth
    self.job = None
  

  def _media_download(self, request, chunksize, encoding=None):
    data = BytesIO()
    leftovers = b''
  
    media = MediaIoBaseDownload(data, request, chunksize=chunksize)
  
    retries = 0
    done = False
    while not done:
      error = None
      try:
        progress, done = media.next_chunk()
        if progress:
          print('Download %d%%' % int(progress.progress() * 100))
  
        data.seek(0)
  
        if encoding is None:
          yield data.read()
  
        elif encoding.lower() == 'utf-8':
          position = find_utf8_split(data)
          yield (leftovers + data.read(position)).decode(encoding)
          leftftovers = data.read()
  
        else:
          yield data.read().decode(encoding)
  
        data.seek(0)
        data.truncate(0)
      except HttpError as err:
        error = err
        if err.resp.status < 500:
          raise
      except (httplib2.HttpLib2Error, IOError) as err:
        error = err
  
      if error:
        retries += 1
        if retries > RETRIES:
          raise error
        else:
          sleep(5 * retries)
      else:
        retries = 0
  
    print('Download 100%')
  
  
  def object_exists(self, bucket, filename):
    try:
      API_Storage(self.config, self.auth).objects().get(bucket=bucket, object=filename).execute()
      return True
    except HttpError as e:
      if e.resp.status == 404:
        return False
      else:
        raise
  
  
  def object_get(self, bucket, filename):
    try:
      return API_Storage(self.config, self.auth).objects().get_media(bucket=bucket, object=filename).execute()
    except HttpError as e:
      if e.resp.status == 404:
        return None
      else:
        raise
  
  
  def object_get_chunks(self, bucket, filename, chunksize=STORAGE_CHUNKSIZE, encoding=None):
    data = BytesIO()
    request = API_Storage(self.config, self.auth).objects().get_media(bucket=bucket, object=filename).execute(run=False)
    yield from self._media_download(request, chunksize, encoding)
  
  
  def object_put(self, bucket, filename, data, mimetype=None):
    if mimetype is None:
      mimetype = mimetypes.guess_type(filename)[0] or 'application/mime'

    media = MediaIoBaseUpload(data, mimetype=mimetype, chunksize=STORAGE_CHUNKSIZE, resumable=True)
    request = API_Storage(self.config, self.auth).objects().insert(bucket=bucket, name=filename, media_body=media).execute(run=False)
  
    response = None
    errors = 0
    while response is None:
      error = None
      try:
        status, response = request.next_chunk()
        if self.config.verbose and status:
          print('Uploaded %d%%.' % int(status.progress() * 100))
      except HttpError as e:
        if e.resp.status < 500:
          raise
        error = e
      except (httplib2.HttpLib2Error, IOError) as e:
        error = e
  
      errors = (errors + 1) if error else 0
      if errors > RETRIES:
        raise error
  
    if self.config.verbose:
      print('Uploaded 100%.')
  
  
  def object_list(self, bucket, prefix, raw=False, files_only=False):
    for item in API_Storage(self.config, self.auth, iterate=True).objects().list(bucket=bucket, prefix=prefix).execute():
      if files_only and item['name'].endswith('/'):
        continue
      yield item if raw else '%s:%s' % (bucket, item['name'])
  
  
  def object_copy(self, path_from, path_to):
    from_bucket, from_filename = path_from.split(':', 1)
    to_bucket, to_filename = path_to.split(':', 1)
  
    body = {
      'kind': 'storage#object',
      'bucket': to_bucket,
      'name': to_filename,
      'storageClass': 'REGIONAL',
    }
  
    return API_Storage(self.config, self.auth).objects().rewrite(
      sourceBucket=from_bucket,
      sourceObject=from_filename,
      destinationBucket=to_bucket,
      destinationObject=to_filename,
      body=body
    ).execute()
  
  
  def object_delete(self, bucket, filename):
    try:
      return API_Storage(self.config, self.auth).objects().delete(bucket=bucket, object=filename).execute()
    except HttpError as e:
      if json.loads(e.content.decode())['error']['code'] == 404:
        return None
      else:
        raise
  
  
  def object_move(self, path_from, path_to):
    self.object_copy(self.config, self.auth, path_from, path_to)
    self.object_delete(self.config, self.auth, path_from)
  
  
  def bucket_get(self, name):
    return API_Storage(self.config, self.auth).buckets().get(bucket=name).execute()
  
  
  def bucket_create(self, name, location='us-west1'):
    if self.bucket_get(self.config, self.auth, name) is None:
      body = {
        'kind': 'storage#bucket',
        'name': name,
        'storageClass': 'REGIONAL',
        'location': location,
      }
  
      try:
        return API_Storage(self.config, self.auth).buckets().insert(project=config.project, body=body).execute()
      except HttpError as e:
        if json.loads(e.content.decode())['error']['code'] == 409:
          return API_Storage(self.config, self.auth).buckets().get(bucket=name).execute()
        else:
          raise
  
  
  def bucket_delete(self, name):
    return API_Storage(self.config, self.auth).buckets().delete(bucket=name).execute()
  
  
  #role = OWNER, READER, WRITER
  def bucket_access(self, project, name, role='OWNER', emails=[], groups=[], domains=[]):
    entities = ['user-%s' % e for e in  emails]
    entities += ['group-%s' % e for e in groups]
    entities += ['domain-%s' % e for e in  domains]
  
    for entity in entities:
      body = {
        'kind': 'storage#bucketAccessControl',
        'bucket': name,
        'entity': entity,
        'role': role
      }
      API_Storage(self.config, self.auth).bucketAccessControls().insert(bucket=name, body=body).execute()
  
  
  # Alternative for managing permissions ( overkill? )
  #  if emails or groups or services or groups:
  #    access = service.buckets().getIamPolicy(bucket=name).execute(num_retries=RETRIES)
  #
  #    access['bindings'] = []
  #    for r in role:
  #      access['bindings'].append({
  #        "role":"roles/storage.object%s" % r,
  #        "members": ['user:%s' % m for m in emails] + \
  #                   ['group:%s' % m for m in groups] + \
  #                   ['serviceAccount:%s' % m for m in services] + \
  #                   ['domain:%s' % m for m in domains]
  #      })
  #
  #    job = service.buckets().setIamPolicy(bucket=name, body=access).execute(num_retries=RETRIES)
  #    sleep(1)
