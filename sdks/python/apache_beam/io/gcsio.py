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

import cStringIO
import errno
import fnmatch
import logging
import multiprocessing
import os
import Queue
import re
import threading
import traceback

from apitools.base.py.exceptions import HttpError
from apitools.base.py.batch import BatchApiRequest
import apitools.base.py.transfer as transfer

from apache_beam.internal import auth
from apache_beam.utils import retry

# Issue a friendlier error message if the storage library is not available.
# TODO(silviuc): Remove this guard when storage is available everywhere.
try:
  # pylint: disable=wrong-import-order, wrong-import-position
  from apache_beam.internal.clients import storage
except ImportError:
  raise RuntimeError(
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


def parse_gcs_path(gcs_path):
  """Return the bucket and object names of the given gs:// path."""
  match = re.match('^gs://([^/]+)/(.+)$', gcs_path)
  if match is None:
    raise ValueError('GCS path must be in the form gs://<bucket>/<object>.')
  return match.group(1), match.group(2)


class GcsIOError(IOError, retry.PermanentException):
  """GCS IO error that should not be retried."""
  pass


class GcsIO(object):
  """Google Cloud Storage I/O client."""

  def __new__(cls, storage_client=None):
    if storage_client:
      return super(GcsIO, cls).__new__(cls, storage_client)
    else:
      # Create a single storage client for each thread.  We would like to avoid
      # creating more than one storage client for each thread, since each
      # initialization requires the relatively expensive step of initializing
      # credentaials.
      local_state = threading.local()
      if getattr(local_state, 'gcsio_instance', None) is None:
        credentials = auth.get_service_credentials()
        storage_client = storage.StorageV1(credentials=credentials)
        local_state.gcsio_instance = (
            super(GcsIO, cls).__new__(cls, storage_client))
        local_state.gcsio_instance.client = storage_client
      return local_state.gcsio_instance

  def __init__(self, storage_client=None):
    # We must do this check on storage_client because the client attribute may
    # have already been set in __new__ for the singleton case when
    # storage_client is None.
    if storage_client is not None:
      self.client = storage_client

  def open(self,
           filename,
           mode='r',
           read_buffer_size=DEFAULT_READ_BUFFER_SIZE,
           mime_type='application/octet-stream'):
    """Open a GCS file path for reading or writing.

    Args:
      filename: GCS file path in the form gs://<bucket>/<object>.
      mode: 'r' for reading or 'w' for writing.
      read_buffer_size: Buffer size to use during read operations.
      mime_type: Mime type to set for write operations.

    Returns:
      file object.

    Raises:
      ValueError: Invalid open file mode.
    """
    if mode == 'r' or mode == 'rb':
      return GcsBufferedReader(self.client, filename, mode=mode,
                               buffer_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      return GcsBufferedWriter(self.client, filename, mode=mode,
                               mime_type=mime_type)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def glob(self, pattern, limit=None):
    """Return the GCS path names matching a given path name pattern.

    Path name patterns are those recognized by fnmatch.fnmatch().  The path
    can contain glob characters (*, ?, and [...] sets).

    Args:
      pattern: GCS file path pattern in the form gs://<bucket>/<name_pattern>.

    Returns:
      list of GCS file paths matching the given pattern.
    """
    bucket, name_pattern = parse_gcs_path(pattern)
    # Get the prefix with which we can list objects in the given bucket.
    prefix = re.match('^[^[*?]*', name_pattern).group(0)
    request = storage.StorageObjectsListRequest(bucket=bucket, prefix=prefix)
    object_paths = []
    while True:
      response = self.client.objects.List(request)
      for item in response.items:
        if fnmatch.fnmatch(item.name, name_pattern):
          object_paths.append('gs://%s/%s' % (item.bucket, item.name))
      if response.nextPageToken:
        request.pageToken = response.nextPageToken
        if limit is not None and len(object_paths) >= limit:
          break
      else:
        break
    return object_paths[:limit]

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
        retryable_codes=retry.SERVER_ERROR_OR_TIMEOUT_CODES)
    for path in paths:
      bucket, object_path = parse_gcs_path(path)
      request = storage.StorageObjectsDeleteRequest(
          bucket=bucket, object=object_path)
      batch_request.Add(self.client.objects, 'Delete', request)
    api_calls = batch_request.Execute(self.client._http)  # pylint: disable=protected-access
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
  def copy(self, src, dest):
    """Copies the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    src_bucket, src_path = parse_gcs_path(src)
    dest_bucket, dest_path = parse_gcs_path(dest)
    request = storage.StorageObjectsCopyRequest(
        sourceBucket=src_bucket,
        sourceObject=src_path,
        destinationBucket=dest_bucket,
        destinationObject=dest_path)
    try:
      self.client.objects.Copy(request)
    except HttpError as http_error:
      if http_error.status_code == 404:
        # This is a permanent error that should not be retried.  Note that
        # FileSink.finalize_write expects an IOError when the source file does
        # not exist.
        raise GcsIOError(errno.ENOENT, 'Source file not found: %s' % src)
      raise

  # We intentionally do not decorate this method with a retry, as retrying is
  # handled in BatchApiRequest.Execute().
  def copy_batch(self, src_dest_pairs):
    """Copies the given GCS object from src to dest.

    Args:
      src_dest_pairs: list of (src, dest) tuples of gs://<bucket>/<name> files
                      paths to copy from src to dest, not to exceed
                      MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (src, dest, exception) in the same order as the
             src_dest_pairs argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs:
      return []
    batch_request = BatchApiRequest(
        retryable_codes=retry.SERVER_ERROR_OR_TIMEOUT_CODES)
    for src, dest in src_dest_pairs:
      src_bucket, src_path = parse_gcs_path(src)
      dest_bucket, dest_path = parse_gcs_path(dest)
      request = storage.StorageObjectsCopyRequest(
          sourceBucket=src_bucket,
          sourceObject=src_path,
          destinationBucket=dest_bucket,
          destinationObject=dest_path)
      batch_request.Add(self.client.objects, 'Copy', request)
    api_calls = batch_request.Execute(self.client._http)  # pylint: disable=protected-access
    result_statuses = []
    for i, api_call in enumerate(api_calls):
      src, dest = src_dest_pairs[i]
      exception = None
      if api_call.is_error:
        exception = api_call.exception
        # Translate 404 to the appropriate not found exception.
        if isinstance(exception, HttpError) and exception.status_code == 404:
          exception = (
              GcsIOError(errno.ENOENT, 'Source file not found: %s' % src))
      result_statuses.append((src, dest, exception))
    return result_statuses

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
    for entry in self.glob(src + '*'):
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
  def size_of_files_in_glob(self, pattern):
    """Returns the size of all the files in the glob as a dictionary

    Args:
      path: a file path pattern that reads the size of all the files
    """
    bucket, name_pattern = parse_gcs_path(pattern)
    # Get the prefix with which we can list objects in the given bucket.
    prefix = re.match('^[^[*?]*', name_pattern).group(0)
    request = storage.StorageObjectsListRequest(bucket=bucket, prefix=prefix)
    file_sizes = {}
    while True:
      response = self.client.objects.List(request)
      for item in response.items:
        if fnmatch.fnmatch(item.name, name_pattern):
          file_name = 'gs://%s/%s' % (item.bucket, item.name)
          file_sizes[file_name] = item.size
      if response.nextPageToken:
        request.pageToken = response.nextPageToken
      else:
        break
    return file_sizes


# TODO: Consider using cStringIO instead of buffers and data_lists when reading.
class GcsBufferedReader(object):
  """A class for reading Google Cloud Storage files."""

  def __init__(self,
               client,
               path,
               mode='r',
               buffer_size=DEFAULT_READ_BUFFER_SIZE,
               segment_timeout=DEFAULT_READ_SEGMENT_TIMEOUT_SECONDS):
    self.client = client
    self.path = path
    self.bucket, self.name = parse_gcs_path(path)
    self.mode = mode
    self.buffer_size = buffer_size
    self.segment_timeout = segment_timeout

    # Get object state.
    self.get_request = (storage.StorageObjectsGetRequest(
        bucket=self.bucket, object=self.name))
    try:
      metadata = self._get_object_metadata(self.get_request)
    except HttpError as http_error:
      if http_error.status_code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self.path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self.path,
                      http_error)
        raise
    self.size = metadata.size

    # Ensure read is from file of the correct generation.
    self.get_request.generation = metadata.generation

    # Initialize read buffer state.
    self.download_stream = cStringIO.StringIO()
    self.downloader = transfer.Download(
        self.download_stream, auto_transfer=False, chunksize=self.buffer_size)
    self.client.objects.Get(self.get_request, download=self.downloader)
    self.position = 0
    self.buffer = ''
    self.buffer_start_position = 0
    self.closed = False

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_object_metadata(self, get_request):
    return self.client.objects.Get(get_request)

  def __iter__(self):
    return self

  def __next__(self):
    """Read one line delimited by '\\n' from the file.
    """
    return self.next()

  def next(self):
    """Read one line delimited by '\\n' from the file.
    """
    line = self.readline()
    if not line:
      raise StopIteration
    return line

  def read(self, size=-1):
    """Read data from a GCS file.

    Args:
      size: Number of bytes to read. Actual number of bytes read is always
            equal to size unless EOF is reached. If size is negative or
            unspecified, read the entire file.

    Returns:
      data read as str.

    Raises:
      IOError: When this buffer is closed.
    """
    return self._read_inner(size=size, readline=False)

  def readline(self, size=-1):
    """Read one line delimited by '\\n' from the file.

    Mimics behavior of the readline() method on standard file objects.

    A trailing newline character is kept in the string. It may be absent when a
    file ends with an incomplete line. If the size argument is non-negative,
    it specifies the maximum string size (counting the newline) to return.
    A negative size is the same as unspecified. Empty string is returned
    only when EOF is encountered immediately.

    Args:
      size: Maximum number of bytes to read. If not specified, readline stops
        only on '\\n' or EOF.

    Returns:
      The data read as a string.

    Raises:
      IOError: When this buffer is closed.
    """
    return self._read_inner(size=size, readline=True)

  def _read_inner(self, size=-1, readline=False):
    """Shared implementation of read() and readline()."""
    self._check_open()
    if not self._remaining():
      return ''

    # Prepare to read.
    data_list = []
    if size is None:
      size = -1
    to_read = min(size, self._remaining())
    if to_read < 0:
      to_read = self._remaining()
    break_after = False

    while to_read > 0:
      # If we have exhausted the buffer, get the next segment.
      # TODO(ccy): We should consider prefetching the next block in another
      # thread.
      self._fetch_next_if_buffer_exhausted()

      # Determine number of bytes to read from buffer.
      buffer_bytes_read = self.position - self.buffer_start_position
      bytes_to_read_from_buffer = min(
          len(self.buffer) - buffer_bytes_read, to_read)

      # If readline is set, we only want to read up to and including the next
      # newline character.
      if readline:
        next_newline_position = self.buffer.find('\n', buffer_bytes_read,
                                                 len(self.buffer))
        if next_newline_position != -1:
          bytes_to_read_from_buffer = (
              1 + next_newline_position - buffer_bytes_read)
          break_after = True

      # Read bytes.
      data_list.append(self.buffer[buffer_bytes_read:buffer_bytes_read +
                                   bytes_to_read_from_buffer])
      self.position += bytes_to_read_from_buffer
      to_read -= bytes_to_read_from_buffer

      if break_after:
        break

    return ''.join(data_list)

  def _fetch_next_if_buffer_exhausted(self):
    if not self.buffer or (
        self.buffer_start_position + len(self.buffer) <= self.position):
      bytes_to_request = min(self._remaining(), self.buffer_size)
      self.buffer_start_position = self.position
      retry_count = 0
      while retry_count <= 10:
        queue = Queue.Queue()
        t = threading.Thread(target=self._fetch_to_queue,
                             args=(queue, self._get_segment,
                                   (self.position, bytes_to_request)))
        t.daemon = True
        t.start()
        try:
          result, exn, tb = queue.get(timeout=self.segment_timeout)
        except Queue.Empty:
          logging.warning(
              ('Timed out fetching %d bytes from position %d of %s after %f '
               'seconds; retrying...'), bytes_to_request, self.position,
              self.path, self.segment_timeout)
          retry_count += 1
          # Reinitialize download objects.
          self.download_stream = cStringIO.StringIO()
          self.downloader = transfer.Download(
              self.download_stream, auto_transfer=False,
              chunksize=self.buffer_size)
          self.client.objects.Get(self.get_request, download=self.downloader)
          continue
        if exn:
          logging.error(
              ('Exception while fetching %d bytes from position %d of %s: '
               '%s\n%s'),
              bytes_to_request, self.position, self.path, exn, tb)
          raise exn
        self.buffer = result
        return
      raise GcsIOError(
          'Reached retry limit for _fetch_next_if_buffer_exhausted.')

  def _fetch_to_queue(self, queue, func, args):
    try:
      value = func(*args)
      queue.put((value, None, None))
    except Exception as e:  # pylint: disable=broad-except
      tb = traceback.format_exc()
      queue.put((None, e, tb))

  def _remaining(self):
    return self.size - self.position

  def close(self):
    """Close the current GCS file."""
    self.closed = True
    self.download_stream = None
    self.downloader = None
    self.buffer = None

  def _get_segment(self, start, size):
    """Get the given segment of the current GCS file."""
    if size == 0:
      return ''
    # The objects self.downloader and self.download_stream may be recreated if
    # this call times out; we save them locally to avoid any threading issues.
    downloader = self.downloader
    download_stream = self.download_stream
    end = start + size - 1
    downloader.GetRange(start, end)
    value = download_stream.getvalue()
    # Clear the cStringIO object after we've read its contents.
    download_stream.truncate(0)
    assert len(value) == size
    return value

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current offset.

    Note if the new offset is out of bound, it is adjusted to either 0 or EOF.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        os.SEEK_CUR (seek relative to the current position), and os.SEEK_END
        (seek relative to the end, offset should be negative).

    Raises:
      IOError: When this buffer is closed.
      ValueError: When whence is invalid.
    """
    self._check_open()

    self.buffer = ''
    self.buffer_start_position = -1

    if whence == os.SEEK_SET:
      self.position = offset
    elif whence == os.SEEK_CUR:
      self.position += offset
    elif whence == os.SEEK_END:
      self.position = self.size + offset
    else:
      raise ValueError('Whence mode %r is invalid.' % whence)

    self.position = min(self.position, self.size)
    self.position = max(self.position, 0)

  def tell(self):
    """Tell the file's current offset.

    Returns:
      current offset in reading this file.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    return self.position

  def _check_open(self):
    if self.closed:
      raise IOError('Buffer is closed.')

  def seekable(self):
    return True

  def readable(self):
    return True

  def writable(self):
    return False


# TODO: Consider using cStringIO instead of buffers and data_lists when reading
# and writing.
class GcsBufferedWriter(object):
  """A class for writing Google Cloud Storage files."""

  class PipeStream(object):
    """A class that presents a pipe connection as a readable stream."""

    def __init__(self, recv_pipe):
      self.conn = recv_pipe
      self.closed = False
      self.position = 0
      self.remaining = ''

    def read(self, size):
      """Read data from the wrapped pipe connection.

      Args:
        size: Number of bytes to read. Actual number of bytes read is always
              equal to size unless EOF is reached.

      Returns:
        data read as str.
      """
      data_list = []
      bytes_read = 0
      while bytes_read < size:
        bytes_from_remaining = min(size - bytes_read, len(self.remaining))
        data_list.append(self.remaining[0:bytes_from_remaining])
        self.remaining = self.remaining[bytes_from_remaining:]
        self.position += bytes_from_remaining
        bytes_read += bytes_from_remaining
        if not self.remaining:
          try:
            self.remaining = self.conn.recv_bytes()
          except EOFError:
            break
      return ''.join(data_list)

    def tell(self):
      """Tell the file's current offset.

      Returns:
        current offset in reading this file.

      Raises:
        IOError: When this stream is closed.
      """
      self._check_open()
      return self.position

    def seek(self, offset, whence=os.SEEK_SET):
      # The apitools.base.py.transfer.Upload class insists on seeking to the end
      # of a stream to do a check before completing an upload, so we must have
      # this no-op method here in that case.
      if whence == os.SEEK_END and offset == 0:
        return
      elif whence == os.SEEK_SET and offset == self.position:
        return
      raise NotImplementedError

    def _check_open(self):
      if self.closed:
        raise IOError('Stream is closed.')

  def __init__(self,
               client,
               path,
               mode='w',
               mime_type='application/octet-stream'):
    self.client = client
    self.path = path
    self.mode = mode
    self.bucket, self.name = parse_gcs_path(path)
    self.mode = mode

    self.closed = False
    self.position = 0

    # A small buffer to avoid CPU-heavy per-write pipe calls.
    self.write_buffer = bytearray()
    self.write_buffer_size = 128 * 1024

    # Set up communication with uploading thread.
    parent_conn, child_conn = multiprocessing.Pipe()
    self.child_conn = child_conn
    self.conn = parent_conn

    # Set up uploader.
    self.insert_request = (storage.StorageObjectsInsertRequest(
        bucket=self.bucket, name=self.name))
    self.upload = transfer.Upload(
        GcsBufferedWriter.PipeStream(child_conn),
        mime_type,
        chunksize=WRITE_CHUNK_SIZE)
    self.upload.strategy = transfer.RESUMABLE_UPLOAD

    # Start uploading thread.
    self.upload_thread = threading.Thread(target=self._start_upload)
    self.upload_thread.daemon = True
    self.upload_thread.last_error = None
    self.upload_thread.start()

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
      self.client.objects.Insert(self.insert_request, upload=self.upload)
    except Exception as e:  # pylint: disable=broad-except
      logging.error('Error in _start_upload while inserting file %s: %s',
                    self.path, traceback.format_exc())
      self.upload_thread.last_error = e
    finally:
      self.child_conn.close()

  def write(self, data):
    """Write data to a GCS file.

    Args:
      data: data to write as str.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    if not data:
      return
    self.write_buffer.extend(data)
    if len(self.write_buffer) > self.write_buffer_size:
      self._flush_write_buffer()
    self.position += len(data)

  def flush(self):
    """Flushes any internal buffer to the underlying GCS file."""
    self._check_open()
    self._flush_write_buffer()

  def tell(self):
    """Return the total number of bytes passed to write() so far."""
    return self.position

  def close(self):
    """Close the current GCS file."""
    if self.closed:
      logging.warn('Channel for %s is not open.', self.path)
      return

    self._flush_write_buffer()
    self.closed = True
    self.conn.close()
    self.upload_thread.join()
    # Check for exception since the last _flush_write_buffer() call.
    if self.upload_thread.last_error:
      raise self.upload_thread.last_error  # pylint: disable=raising-bad-type

  def __enter__(self):
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.close()

  def _check_open(self):
    if self.closed:
      raise IOError('Buffer is closed.')

  def seekable(self):
    return False

  def readable(self):
    return False

  def writable(self):
    return True

  def _flush_write_buffer(self):
    try:
      self.conn.send_bytes(buffer(self.write_buffer))
      self.write_buffer = bytearray()
    except IOError:
      if self.upload_thread.last_error:
        raise self.upload_thread.last_error  # pylint: disable=raising-bad-type
      else:
        raise
