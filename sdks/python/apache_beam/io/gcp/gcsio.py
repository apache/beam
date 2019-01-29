#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Google Cloud Storage client.

This library evolved from the Google App Engine GCS client available at
https://github.com/GoogleCloudPlatform/appengine-gcs-client.
"""

from __future__ import absolute_import

import errno
import io
import logging
import multiprocessing
import re
import sys
import threading
import time
import traceback
from builtins import object

from apache_beam.internal.http_client import get_new_http
from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystemio import PipeStream
from apache_beam.io.filesystemio import Uploader
from apache_beam.io.filesystemio import UploaderStream
from apache_beam.utils import retry

__all__ = ['GcsIO']


# Issue a friendlier error message if the storage library is not available.
# TODO(silviuc): Remove this guard when storage is available everywhere.
try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  import apitools.base.py.transfer as transfer
  from apitools.base.py.batch import BatchApiRequest
  from apitools.base.py.exceptions import HttpError
  from apache_beam.internal.gcp import auth
  from apache_beam.io.gcp.internal.clients import storage
except ImportError:
  raise ImportError(
      'Google Cloud Storage I/O not supported for this execution environment '
      '(could not import storage API client).')

# This is the size of each partial-file read operation from GCS.  This
# parameter was chosen to give good throughput while keeping memory usage at
# a reasonable level; the following table shows throughput reached when
# reading files of a given size with a chosen buffer size and informed the
# choice of the value, as of 11/2016:
#
# +---------------+------------+-------------+-------------+-------------+
# |               | 50 MB file | 100 MB file | 200 MB file | 400 MB file |
# +---------------+------------+-------------+-------------+-------------+
# | 8 MB buffer   | 17.12 MB/s | 22.67 MB/s  | 23.81 MB/s  | 26.05 MB/s  |
# | 16 MB buffer  | 24.21 MB/s | 42.70 MB/s  | 42.89 MB/s  | 46.92 MB/s  |
# | 32 MB buffer  | 28.53 MB/s | 48.08 MB/s  | 54.30 MB/s  | 54.65 MB/s  |
# | 400 MB buffer | 34.72 MB/s | 71.13 MB/s  | 79.13 MB/s  | 85.39 MB/s  |
# +---------------+------------+-------------+-------------+-------------+
DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024

# This is the number of seconds the library will wait for a partial-file read
# operation from GCS to complete before retrying.
DEFAULT_READ_SEGMENT_TIMEOUT_SECONDS = 60

# This is the size of chunks used when writing to GCS.
WRITE_CHUNK_SIZE = 8 * 1024 * 1024


# Maximum number of operations permitted in GcsIO.copy_batch() and
# GcsIO.delete_batch().
MAX_BATCH_OPERATION_SIZE = 100

# Batch endpoint URL for GCS.
# We have to specify an API specific endpoint here since Google APIs global
# batch endpoints will be deprecated on 03/25/2019.
# See https://developers.googleblog.com/2018/03/discontinuing-support-for-json-rpc-and.html.  # pylint: disable=line-too-long
# Currently apitools library uses a global batch endpoint by default:
# https://github.com/google/apitools/blob/master/apitools/base/py/batch.py#L152
# TODO: remove this constant and it's usage after apitools move to using an API
# specific batch endpoint or after Beam gcsio module start using a GCS client
# library that does not use global batch endpoints.
GCS_BATCH_ENDPOINT = 'https://www.googleapis.com/batch/storage/v1'


def parse_gcs_path(gcs_path, object_optional=False):
  """Return the bucket and object names of the given gs:// path."""
  match = re.match('^gs://([^/]+)/(.*)$', gcs_path)
  if match is None or (match.group(2) == '' and not object_optional):
    raise ValueError('GCS path must be in the form gs://<bucket>/<object>.')
  return match.group(1), match.group(2)


class GcsIOError(IOError, retry.PermanentException):
  """GCS IO error that should not be retried."""
  pass


class GcsIO(object):
  """Google Cloud Storage I/O client."""

  def __new__(cls, storage_client=None):
    if storage_client:
      # This path is only used for testing.
      return super(GcsIO, cls).__new__(cls)
    else:
      # Create a single storage client for each thread.  We would like to avoid
      # creating more than one storage client for each thread, since each
      # initialization requires the relatively expensive step of initializing
      # credentaials.
      local_state = threading.local()
      if getattr(local_state, 'gcsio_instance', None) is None:
        credentials = auth.get_service_credentials()
        storage_client = storage.StorageV1(
            credentials=credentials,
            get_credentials=False,
            http=get_new_http(),
            response_encoding=None if sys.version_info[0] < 3 else 'utf8')
        local_state.gcsio_instance = super(GcsIO, cls).__new__(cls)
        local_state.gcsio_instance.client = storage_client
      return local_state.gcsio_instance

  def __init__(self, storage_client=None):
    # We must do this check on storage_client because the client attribute may
    # have already been set in __new__ for the singleton case when
    # storage_client is None.
    if storage_client is not None:
      self.client = storage_client
    self._rewrite_cb = None

  def _set_rewrite_response_callback(self, callback):
    """For testing purposes only. No backward compatibility guarantees.

    Args:
      callback: A function that receives ``storage.RewriteResponse``.
    """
    self._rewrite_cb = callback

  def open(self,
           filename,
           mode='r',
           read_buffer_size=DEFAULT_READ_BUFFER_SIZE,
           mime_type='application/octet-stream'):
    """Open a GCS file path for reading or writing.

    Args:
      filename (str): GCS file path in the form ``gs://<bucket>/<object>``.
      mode (str): ``'r'`` for reading or ``'w'`` for writing.
      read_buffer_size (int): Buffer size to use during read operations.
      mime_type (str): Mime type to set for write operations.

    Returns:
      GCS file object.

    Raises:
      ~exceptions.ValueError: Invalid open file mode.
    """
    if mode == 'r' or mode == 'rb':
      downloader = GcsDownloader(self.client, filename,
                                 buffer_size=read_buffer_size)
      return io.BufferedReader(DownloaderStream(downloader, mode=mode),
                               buffer_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      uploader = GcsUploader(self.client, filename, mime_type)
      return io.BufferedWriter(UploaderStream(uploader, mode=mode),
                               buffer_size=128 * 1024)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def delete(self, path):
    """Deletes the object at the given GCS path.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    bucket, object_path = parse_gcs_path(path)
    request = storage.StorageObjectsDeleteRequest(
        bucket=bucket, object=object_path)
    try:
      self.client.objects.Delete(request)
    except HttpError as http_error:
      if http_error.status_code == 404:
        # Return success when the file doesn't exist anymore for idempotency.
        return
      raise

  # We intentionally do not decorate this method with a retry, as retrying is
  # handled in BatchApiRequest.Execute().
  def delete_batch(self, paths):
    """Deletes the objects at the given GCS paths.

    Args:
      paths: List of GCS file path patterns in the form gs://<bucket>/<name>,
             not to exceed MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (path, exception) in the same order as the paths
             argument, where exception is None if the operation succeeded or
             the relevant exception if the operation failed.
    """
    if not paths:
      return []
    batch_request = BatchApiRequest(
        batch_url=GCS_BATCH_ENDPOINT,
        retryable_codes=retry.SERVER_ERROR_OR_TIMEOUT_CODES,
        response_encoding='utf-8')
    for path in paths:
      bucket, object_path = parse_gcs_path(path)
      request = storage.StorageObjectsDeleteRequest(
          bucket=bucket, object=object_path)
      batch_request.Add(self.client.objects, 'Delete', request)
    api_calls = batch_request.Execute(self.client._http) # pylint: disable=protected-access
    result_statuses = []
    for i, api_call in enumerate(api_calls):
      path = paths[i]
      exception = None
      if api_call.is_error:
        exception = api_call.exception
        # Return success when the file doesn't exist anymore for idempotency.
        if isinstance(exception, HttpError) and exception.status_code == 404:
          exception = None
      result_statuses.append((path, exception))
    return result_statuses

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def copy(self, src, dest, dest_kms_key_name=None,
           max_bytes_rewritten_per_call=None):
    """Copies the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.
      dest_kms_key_name: Experimental. No backwards compatibility guarantees.
        Encrypt dest with this Cloud KMS key. If None, will use dest bucket
        encryption defaults.
      max_bytes_rewritten_per_call: Experimental. No backwards compatibility
        guarantees. Each rewrite API call will return after these many bytes.
        Used for testing.

    Raises:
      TimeoutError on timeout.
    """
    src_bucket, src_path = parse_gcs_path(src)
    dest_bucket, dest_path = parse_gcs_path(dest)
    request = storage.StorageObjectsRewriteRequest(
        sourceBucket=src_bucket,
        sourceObject=src_path,
        destinationBucket=dest_bucket,
        destinationObject=dest_path,
        destinationKmsKeyName=dest_kms_key_name,
        maxBytesRewrittenPerCall=max_bytes_rewritten_per_call)
    response = self.client.objects.Rewrite(request)
    while not response.done:
      logging.debug(
          'Rewrite progress: %d of %d bytes, %s to %s',
          response.totalBytesRewritten, response.objectSize, src, dest)
      request.rewriteToken = response.rewriteToken
      response = self.client.objects.Rewrite(request)
      if self._rewrite_cb is not None:
        self._rewrite_cb(response)

    logging.debug('Rewrite done: %s to %s', src, dest)

  # We intentionally do not decorate this method with a retry, as retrying is
  # handled in BatchApiRequest.Execute().
  def copy_batch(self, src_dest_pairs, dest_kms_key_name=None,
                 max_bytes_rewritten_per_call=None):
    """Copies the given GCS object from src to dest.

    Args:
      src_dest_pairs: list of (src, dest) tuples of gs://<bucket>/<name> files
                      paths to copy from src to dest, not to exceed
                      MAX_BATCH_OPERATION_SIZE in length.
      dest_kms_key_name: Experimental. No backwards compatibility guarantees.
        Encrypt dest with this Cloud KMS key. If None, will use dest bucket
        encryption defaults.
      max_bytes_rewritten_per_call: Experimental. No backwards compatibility
        guarantees. Each rewrite call will return after these many bytes. Used
        primarily for testing.

    Returns: List of tuples of (src, dest, exception) in the same order as the
             src_dest_pairs argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs:
      return []
    pair_to_request = {}
    for pair in src_dest_pairs:
      src_bucket, src_path = parse_gcs_path(pair[0])
      dest_bucket, dest_path = parse_gcs_path(pair[1])
      request = storage.StorageObjectsRewriteRequest(
          sourceBucket=src_bucket,
          sourceObject=src_path,
          destinationBucket=dest_bucket,
          destinationObject=dest_path,
          destinationKmsKeyName=dest_kms_key_name,
          maxBytesRewrittenPerCall=max_bytes_rewritten_per_call)
      pair_to_request[pair] = request
    pair_to_status = {}
    while True:
      pairs_in_batch = list(set(src_dest_pairs) - set(pair_to_status))
      if not pairs_in_batch:
        break
      batch_request = BatchApiRequest(
          batch_url=GCS_BATCH_ENDPOINT,
          retryable_codes=retry.SERVER_ERROR_OR_TIMEOUT_CODES,
          response_encoding='utf-8')
      for pair in pairs_in_batch:
        batch_request.Add(self.client.objects, 'Rewrite', pair_to_request[pair])
      api_calls = batch_request.Execute(self.client._http)  # pylint: disable=protected-access
      for pair, api_call in zip(pairs_in_batch, api_calls):
        src, dest = pair
        response = api_call.response
        if self._rewrite_cb is not None:
          self._rewrite_cb(response)
        if api_call.is_error:
          exception = api_call.exception
          # Translate 404 to the appropriate not found exception.
          if isinstance(exception, HttpError) and exception.status_code == 404:
            exception = (
                GcsIOError(errno.ENOENT, 'Source file not found: %s' % src))
          pair_to_status[pair] = exception
        elif not response.done:
          logging.debug(
              'Rewrite progress: %d of %d bytes, %s to %s',
              response.totalBytesRewritten, response.objectSize, src, dest)
          pair_to_request[pair].rewriteToken = response.rewriteToken
        else:
          logging.debug('Rewrite done: %s to %s', src, dest)
          pair_to_status[pair] = None

    return [(pair[0], pair[1], pair_to_status[pair]) for pair in src_dest_pairs]

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def copytree(self, src, dest):
    """Renames the given GCS "directory" recursively from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>/.
      dest: GCS file path pattern in the form gs://<bucket>/<name>/.
    """
    assert src.endswith('/')
    assert dest.endswith('/')
    for entry in self.list_prefix(src):
      rel_path = entry[len(src):]
      self.copy(entry, dest + rel_path)

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def rename(self, src, dest):
    """Renames the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    self.copy(src, dest)
    self.delete(src)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def exists(self, path):
    """Returns whether the given GCS object exists.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    bucket, object_path = parse_gcs_path(path)
    try:
      request = storage.StorageObjectsGetRequest(
          bucket=bucket, object=object_path)
      self.client.objects.Get(request)  # metadata
      return True
    except HttpError as http_error:
      if http_error.status_code == 404:
        # HTTP 404 indicates that the file did not exist
        return False
      else:
        # We re-raise all other exceptions
        raise

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def checksum(self, path):
    """Looks up the checksum of a GCS object.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    bucket, object_path = parse_gcs_path(path)
    request = storage.StorageObjectsGetRequest(
        bucket=bucket, object=object_path)
    return self.client.objects.Get(request).crc32c

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def size(self, path):
    """Returns the size of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: size of the GCS object in bytes.
    """
    bucket, object_path = parse_gcs_path(path)
    request = storage.StorageObjectsGetRequest(
        bucket=bucket, object=object_path)
    return self.client.objects.Get(request).size

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def kms_key(self, path):
    """Returns the KMS key of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: KMS key name of the GCS object as a string, or None if it doesn't
      have one.
    """
    bucket, object_path = parse_gcs_path(path)
    request = storage.StorageObjectsGetRequest(
        bucket=bucket, object=object_path)
    return self.client.objects.Get(request).kmsKeyName

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def last_updated(self, path):
    """Returns the last updated epoch time of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: last updated time of the GCS object in second.
    """
    bucket, object_path = parse_gcs_path(path)
    request = storage.StorageObjectsGetRequest(
        bucket=bucket, object=object_path)
    datetime = self.client.objects.Get(request).updated
    return (time.mktime(datetime.timetuple()) - time.timezone
            + datetime.microsecond / 1000000.0)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def list_prefix(self, path):
    """Lists files matching the prefix.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/[name].

    Returns:
      Dictionary of file name -> size.
    """
    bucket, prefix = parse_gcs_path(path, object_optional=True)
    request = storage.StorageObjectsListRequest(bucket=bucket, prefix=prefix)
    file_sizes = {}
    counter = 0
    start_time = time.time()
    logging.info("Starting the size estimation of the input")
    while True:
      response = self.client.objects.List(request)
      for item in response.items:
        file_name = 'gs://%s/%s' % (item.bucket, item.name)
        file_sizes[file_name] = item.size
        counter += 1
        if counter % 10000 == 0:
          logging.info("Finished computing size of: %s files", len(file_sizes))
      if response.nextPageToken:
        request.pageToken = response.nextPageToken
      else:
        break
    logging.info("Finished listing %s files in %s seconds.",
                 counter, time.time() - start_time)
    return file_sizes


class GcsDownloader(Downloader):
  def __init__(self, client, path, buffer_size):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_gcs_path(path)
    self._buffer_size = buffer_size

    # Get object state.
    self._get_request = (storage.StorageObjectsGetRequest(
        bucket=self._bucket, object=self._name))
    try:
      metadata = self._get_object_metadata(self._get_request)
    except HttpError as http_error:
      if http_error.status_code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self._path,
                      http_error)
        raise
    self._size = metadata.size

    # Ensure read is from file of the correct generation.
    self._get_request.generation = metadata.generation

    # Initialize read buffer state.
    self._download_stream = io.BytesIO()
    self._downloader = transfer.Download(
        self._download_stream, auto_transfer=False, chunksize=self._buffer_size)
    self._client.objects.Get(self._get_request, download=self._downloader)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_object_metadata(self, get_request):
    return self._client.objects.Get(get_request)

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    self._download_stream.seek(0)
    self._download_stream.truncate(0)
    self._downloader.GetRange(start, end - 1)
    return self._download_stream.getvalue()


class GcsUploader(Uploader):
  def __init__(self, client, path, mime_type):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_gcs_path(path)
    self._mime_type = mime_type

    # Set up communication with child thread.
    parent_conn, child_conn = multiprocessing.Pipe()
    self._child_conn = child_conn
    self._conn = parent_conn

    # Set up uploader.
    self._insert_request = (storage.StorageObjectsInsertRequest(
        bucket=self._bucket, name=self._name))
    self._upload = transfer.Upload(
        PipeStream(self._child_conn),
        self._mime_type,
        chunksize=WRITE_CHUNK_SIZE)
    self._upload.strategy = transfer.RESUMABLE_UPLOAD

    # Start uploading thread.
    self._upload_thread = threading.Thread(target=self._start_upload)
    self._upload_thread.daemon = True
    self._upload_thread.last_error = None
    self._upload_thread.start()

  # TODO(silviuc): Refactor so that retry logic can be applied.
  # There is retry logic in the underlying transfer library but we should make
  # it more explicit so we can control the retry parameters.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _start_upload(self):
    # This starts the uploader thread.  We are forced to run the uploader in
    # another thread because the apitools uploader insists on taking a stream
    # as input. Happily, this also means we get asynchronous I/O to GCS.
    #
    # The uploader by default transfers data in chunks of 1024 * 1024 bytes at
    # a time, buffering writes until that size is reached.
    try:
      self._client.objects.Insert(self._insert_request, upload=self._upload)
    except Exception as e:  # pylint: disable=broad-except
      logging.error('Error in _start_upload while inserting file %s: %s',
                    self._path, traceback.format_exc())
      self._upload_thread.last_error = e
    finally:
      self._child_conn.close()

  def put(self, data):
    try:
      self._conn.send_bytes(data.tobytes())
    except EOFError:
      if self._upload_thread.last_error is not None:
        raise self._upload_thread.last_error # pylint: disable=raising-bad-type
      raise

  def finish(self):
    self._conn.close()
    # TODO(udim): Add timeout=DEFAULT_HTTP_TIMEOUT_SECONDS * 2 and raise if
    # isAlive is True.
    self._upload_thread.join()
    # Check for exception since the last put() call.
    if self._upload_thread.last_error is not None:
      raise self._upload_thread.last_error  # pylint: disable=raising-bad-type
