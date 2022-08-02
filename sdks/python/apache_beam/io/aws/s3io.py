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

"""AWS S3 client
"""

# pytype: skip-file

import errno
import io
import logging
import re
import time
import traceback

from apache_beam.io.aws.clients.s3 import messages
from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystemio import Uploader
from apache_beam.io.filesystemio import UploaderStream
from apache_beam.utils import retry

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  from apache_beam.io.aws.clients.s3 import boto3_client
  BOTO3_INSTALLED = True
except ImportError:
  BOTO3_INSTALLED = False

MAX_BATCH_OPERATION_SIZE = 100


def parse_s3_path(s3_path, object_optional=False):
  """Return the bucket and object names of the given s3:// path."""
  match = re.match('^s3://([^/]+)/(.*)$', s3_path)
  if match is None or (match.group(2) == '' and not object_optional):
    raise ValueError('S3 path must be in the form s3://<bucket>/<object>.')
  return match.group(1), match.group(2)


class S3IO(object):
  """S3 I/O client."""
  def __init__(self, client=None, options=None):
    if client is None and options is None:
      raise ValueError('Must provide one of client or options')
    if client is not None:
      self.client = client
    elif BOTO3_INSTALLED:
      self.client = boto3_client.Client(options=options)
    else:
      message = 'AWS dependencies are not installed, and no alternative ' \
      'client was provided to S3IO.'
      raise RuntimeError(message)

  def open(
      self,
      filename,
      mode='r',
      read_buffer_size=16 * 1024 * 1024,
      mime_type='application/octet-stream'):
    """Open an S3 file path for reading or writing.

    Args:
      filename (str): S3 file path in the form ``s3://<bucket>/<object>``.
      mode (str): ``'r'`` for reading or ``'w'`` for writing.
      read_buffer_size (int): Buffer size to use during read operations.
      mime_type (str): Mime type to set for write operations.

    Returns:
      S3 file object.

    Raises:
      ValueError: Invalid open file mode.
    """
    if mode == 'r' or mode == 'rb':
      downloader = S3Downloader(
          self.client, filename, buffer_size=read_buffer_size)
      return io.BufferedReader(
          DownloaderStream(downloader, mode=mode), buffer_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      uploader = S3Uploader(self.client, filename, mime_type)
      return io.BufferedWriter(
          UploaderStream(uploader, mode=mode), buffer_size=128 * 1024)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def list_prefix(self, path, with_metadata=False):
    """Lists files matching the prefix.

    Args:
      path: S3 file path pattern in the form s3://<bucket>/[name].
      with_metadata: Experimental. Specify whether returns file metadata.

    Returns:
      If ``with_metadata`` is False: dict of file name -> size; if
        ``with_metadata`` is True: dict of file name -> tuple(size, timestamp).
    """
    bucket, prefix = parse_s3_path(path, object_optional=True)
    request = messages.ListRequest(bucket=bucket, prefix=prefix)

    file_info = {}
    counter = 0
    start_time = time.time()

    if with_metadata:
      logging.debug("Starting the file information of the input")
    else:
      logging.debug("Starting the size estimation of the input")

    while True:
      #The list operation will raise an exception
      #when trying to list a nonexistent S3 path.
      #This should not be an issue here.
      #Ignore this exception or it will break the procedure.
      try:
        response = self.client.list(request)
      except messages.S3ClientError as e:
        if e.code == 404:
          break
        else:
          raise e

      for item in response.items:
        file_name = 's3://%s/%s' % (bucket, item.key)
        if with_metadata:
          file_info[file_name] = (
              item.size, self._updated_to_seconds(item.last_modified))
        else:
          file_info[file_name] = item.size
        counter += 1
        if counter % 10000 == 0:
          if with_metadata:
            logging.info(
                "Finished computing file information of: %s files",
                len(file_info))
          else:
            logging.info("Finished computing size of: %s files", len(file_info))
      if response.next_token:
        request.continuation_token = response.next_token
      else:
        break

    logging.log(
        # do not spam logs when list_prefix is likely used to check empty folder
        logging.INFO if counter > 0 else logging.DEBUG,
        "Finished listing %s files in %s seconds.",
        counter,
        time.time() - start_time)

    return file_info

  def checksum(self, path):
    """Looks up the checksum of an S3 object.

    Args:
      path: S3 file path pattern in the form s3://<bucket>/<name>.
    """
    return self._s3_object(path).etag

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def copy(self, src, dest):
    """Copies a single S3 file object from src to dest.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>.
      dest: S3 file path pattern in the form s3://<bucket>/<name>.

    Raises:
      TimeoutError: on timeout.
    """
    src_bucket, src_key = parse_s3_path(src)
    dest_bucket, dest_key = parse_s3_path(dest)
    request = messages.CopyRequest(src_bucket, src_key, dest_bucket, dest_key)
    self.client.copy(request)

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def copy_paths(self, src_dest_pairs):
    """Copies the given S3 objects from src to dest. This can handle directory
    or file paths.

    Args:
      src_dest_pairs: list of (src, dest) tuples of s3://<bucket>/<name> file
                      paths to copy from src to dest
    Returns: List of tuples of (src, dest, exception) in the same order as the
            src_dest_pairs argument, where exception is None if the operation
            succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs: return []

    results = []

    for src_path, dest_path in src_dest_pairs:

      # Copy a directory with self.copy_tree
      if src_path.endswith('/') and dest_path.endswith('/'):
        try:
          results += self.copy_tree(src_path, dest_path)
        except messages.S3ClientError as err:
          results.append((src_path, dest_path, err))

      # Copy individual files with self.copy
      elif not src_path.endswith('/') and not dest_path.endswith('/'):
        src_bucket, src_key = parse_s3_path(src_path)
        dest_bucket, dest_key = parse_s3_path(dest_path)
        request = messages.CopyRequest(
            src_bucket, src_key, dest_bucket, dest_key)

        try:
          self.client.copy(request)
          results.append((src_path, dest_path, None))
        except messages.S3ClientError as err:
          results.append((src_path, dest_path, err))

      # Mismatched paths (one directory, one non-directory) get an error result
      else:
        e = messages.S3ClientError(
            "Can't copy mismatched paths (one directory, one non-directory):" +
            ' %s, %s' % (src_path, dest_path),
            400)
        results.append((src_path, dest_path, e))

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def copy_tree(self, src, dest):
    """Renames the given S3 directory and it's contents recursively
    from src to dest.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>/.
      dest: S3 file path pattern in the form s3://<bucket>/<name>/.

    Returns:
      List of tuples of (src, dest, exception) where exception is None if the
      operation succeeded or the relevant exception if the operation failed.
    """
    assert src.endswith('/')
    assert dest.endswith('/')

    results = []
    for entry in self.list_prefix(src):
      rel_path = entry[len(src):]
      try:
        self.copy(entry, dest + rel_path)
        results.append((entry, dest + rel_path, None))
      except messages.S3ClientError as e:
        results.append((entry, dest + rel_path, e))

    return results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def delete(self, path):
    """Deletes a single S3 file object from src to dest.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>/.
      dest: S3 file path pattern in the form s3://<bucket>/<name>/.

    Returns:
      List of tuples of (src, dest, exception) in the same order as the
      src_dest_pairs argument, where exception is None if the operation
      succeeded or the relevant exception if the operation failed.
    """
    bucket, object_path = parse_s3_path(path)
    request = messages.DeleteRequest(bucket, object_path)

    try:
      self.client.delete(request)
    except messages.S3ClientError as e:
      if e.code == 404:
        return  # Same behavior as GCS - don't surface a 404 error
      else:
        logging.error('HTTP error while deleting file %s: %s', path, 3)
        raise e

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_paths(self, paths):
    """Deletes the given S3 objects from src to dest. This can handle directory
    or file paths.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>/.
      dest: S3 file path pattern in the form s3://<bucket>/<name>/.

    Returns:
      List of tuples of (src, dest, exception) in the same order as the
      src_dest_pairs argument, where exception is None if the operation
      succeeded or the relevant exception if the operation failed.
    """
    directories, not_directories = [], []
    for path in paths:
      if path.endswith('/'): directories.append(path)
      else: not_directories.append(path)

    results = {}

    for directory in directories:
      dir_result = dict(self.delete_tree(directory))
      results.update(dir_result)

    not_directory_results = dict(self.delete_files(not_directories))
    results.update(not_directory_results)

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_files(self, paths, max_batch_size=1000):
    """Deletes the given S3 file object from src to dest.

    Args:
      paths: List of S3 file paths in the form s3://<bucket>/<name>
      max_batch_size: Largest number of keys to send to the client to be deleted
      simultaneously

    Returns: List of tuples of (path, exception) in the same order as the paths
             argument, where exception is None if the operation succeeded or
             the relevant exception if the operation failed.
    """
    if not paths: return []

    # Sort paths into bucket: [keys]
    buckets, keys = zip(*[parse_s3_path(path) for path in paths])
    grouped_keys = {bucket: [] for bucket in buckets}
    for bucket, key in zip(buckets, keys):
      grouped_keys[bucket].append(key)

    # For each bucket, delete minibatches of keys
    results = {}
    for bucket, keys in grouped_keys.items():
      for i in range(0, len(keys), max_batch_size):
        minibatch_keys = keys[i:i + max_batch_size]
        results.update(self._delete_minibatch(bucket, minibatch_keys))

    # Organize final results
    final_results = [(path, results[parse_s3_path(path)]) for path in paths]

    return final_results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _delete_minibatch(self, bucket, keys):
    """A helper method. Boto3 allows batch deletions
    for files within the same bucket.

    Args:
      bucket: String bucket name
      keys: List of keys to be deleted in the bucket

    Returns: dict of the form {(bucket, key): error}, where error is None if the
    operation succeeded
    """
    request = messages.DeleteBatchRequest(bucket, keys)
    results = {}
    try:
      response = self.client.delete_batch(request)

      for key in response.deleted:
        results[(bucket, key)] = None

      for key, error in zip(response.failed, response.errors):
        results[(bucket, key)] = error

    except messages.S3ClientError as e:
      for key in keys:
        results[(bucket, key)] = e

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_tree(self, root):
    """Deletes all objects under the given S3 directory.

    Args:
      path: S3 root path in the form s3://<bucket>/<name>/ (ending with a "/")

    Returns: List of tuples of (path, exception), where each path is an object
            under the given root. exception is None if the operation succeeded
            or the relevant exception if the operation failed.
    """
    assert root.endswith('/')

    paths = self.list_prefix(root)
    return self.delete_files(paths)

  def size(self, path):
    """Returns the size of a single S3 object.

    This method does not perform glob expansion. Hence the given path must be
    for a single S3 object.

    Returns: size of the S3 object in bytes.
    """
    return self._s3_object(path).size

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def rename(self, src, dest):
    """Renames the given S3 object from src to dest.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>.
      dest: S3 file path pattern in the form s3://<bucket>/<name>.
    """
    self.copy(src, dest)
    self.delete(src)

  def last_updated(self, path):
    """Returns the last updated epoch time of a single S3 object.

    This method does not perform glob expansion. Hence the given path must be
    for a single S3 object.

    Returns: last updated time of the S3 object in second.
    """
    return self._updated_to_seconds(self._s3_object(path).last_modified)

  def exists(self, path):
    """Returns whether the given S3 object exists.

    Args:
      path: S3 file path pattern in the form s3://<bucket>/<name>.
    """
    try:
      self._s3_object(path)
      return True
    except messages.S3ClientError as e:
      if e.code == 404:
        # HTTP 404 indicates that the file did not exist
        return False
      else:
        # We re-raise all other exceptions
        raise

  def _status(self, path):
    """For internal use only; no backwards-compatibility guarantees.

    Returns supported fields (checksum, last_updated, size) of a single object
    as a dict at once.

    This method does not perform glob expansion. Hence the given path must be
    for a single S3 object.

    Returns: dict of fields of the S3 object.
    """
    s3_object = self._s3_object(path)
    file_status = {}
    if hasattr(s3_object, 'etag'):
      file_status['checksum'] = s3_object.etag
    if hasattr(s3_object, 'last_modified'):
      file_status['last_updated'] = self._updated_to_seconds(
          s3_object.last_modified)
    if hasattr(s3_object, 'size'):
      file_status['size'] = s3_object.size
    return file_status

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _s3_object(self, path):
    """Returns a S3 object metadata for the given path

    This method does not perform glob expansion. Hence the given path must be
    for a single S3 object.

    Returns: S3 object metadata.
    """
    bucket, object = parse_s3_path(path)
    request = messages.GetRequest(bucket, object)
    return self.client.get_object_metadata(request)

  @staticmethod
  def _updated_to_seconds(updated):
    """Helper function transform the updated field of response to seconds"""
    return (
        time.mktime(updated.timetuple()) - time.timezone +
        updated.microsecond / 1000000.0)

  def rename_files(self, src_dest_pairs):
    """Renames the given S3 objects from src to dest.

    Args:
      src_dest_pairs: list of (src, dest) tuples of s3://<bucket>/<name> file
                      paths to rename from src to dest
    Returns: List of tuples of (src, dest, exception) in the same order as the
            src_dest_pairs argument, where exception is None if the operation
            succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs: return []

    # TODO: Throw value error if path has directory
    for src, dest in src_dest_pairs:
      if src.endswith('/') or dest.endswith('/'):
        raise ValueError('Cannot rename a directory')

    copy_results = self.copy_paths(src_dest_pairs)
    paths_to_delete = [src for (src, _, err) in copy_results if err is None]
    delete_results = self.delete_files(paths_to_delete)

    delete_results_dict = {src: err for (src, err) in delete_results}
    rename_results = []
    for src, dest, err in copy_results:
      if err is not None: rename_results.append((src, dest, err))
      elif delete_results_dict[src] is not None:
        rename_results.append((src, dest, delete_results_dict[src]))
      else:
        rename_results.append((src, dest, None))

    return rename_results


class S3Downloader(Downloader):
  def __init__(self, client, path, buffer_size):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_s3_path(path)
    self._buffer_size = buffer_size

    # Get object state.
    self._get_request = (
        messages.GetRequest(bucket=self._bucket, object=self._name))

    try:
      metadata = self._get_object_metadata(self._get_request)

    except messages.S3ClientError as e:
      if e.code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self._path, 3)
        raise

    self._size = metadata.size

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_object_metadata(self, get_request):
    return self._client.get_object_metadata(get_request)

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    return self._client.get_range(self._get_request, start, end)


class S3Uploader(Uploader):
  def __init__(self, client, path, mime_type='application/octet-stream'):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_s3_path(path)
    self._mime_type = mime_type

    self.part_number = 1
    self.buffer = b''

    self.last_error = None

    self.upload_id = None

    self.parts = []

    self._start_upload()

  # There is retry logic in the underlying transfer library but we should make
  # it more explicit so we can control the retry parameters.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _start_upload(self):
    # The uploader by default transfers data in chunks of 1024 * 1024 bytes at
    # a time, buffering writes until that size is reached.
    try:
      request = messages.UploadRequest(
          self._bucket, self._name, self._mime_type)
      response = self._client.create_multipart_upload(request)
      self.upload_id = response.upload_id
    except Exception as e:  # pylint: disable=broad-except
      logging.error(
          'Error in _start_upload while inserting file %s: %s',
          self._path,
          traceback.format_exc())
      self.last_error = e
      raise e

  def put(self, data):

    MIN_WRITE_SIZE = 5 * 1024 * 1024
    MAX_WRITE_SIZE = 5 * 1024 * 1024 * 1024

    # TODO: Byte strings might not be the most performant way to handle this
    self.buffer += data.tobytes()

    while len(self.buffer) >= MIN_WRITE_SIZE:
      # Take the first chunk off the buffer and write it to S3
      chunk = self.buffer[:MAX_WRITE_SIZE]
      self._write_to_s3(chunk)
      # Remove the written chunk from the buffer
      self.buffer = self.buffer[MAX_WRITE_SIZE:]

  def _write_to_s3(self, data):

    try:
      request = messages.UploadPartRequest(
          self._bucket, self._name, self.upload_id, self.part_number, data)
      response = self._client.upload_part(request)
      self.parts.append({
          'ETag': response.etag, 'PartNumber': response.part_number
      })
      self.part_number = self.part_number + 1
    except messages.S3ClientError as e:
      self.last_error = e
      if e.code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self._path, 3)
        raise

  def finish(self):
    if len(self.buffer) > 0:
      # writes with zero length or mid size files between
      # MIN_WRITE_SIZE = 5 * 1024 * 1024
      # MAX_WRITE_SIZE = 5 * 1024 * 1024 * 1024
      # as we will reach this finish() with len(self.buffer) == 0
      # which will fail
      self._write_to_s3(self.buffer)

    if self.last_error is not None:
      raise self.last_error  # pylint: disable=raising-bad-type

    request = messages.CompleteMultipartUploadRequest(
        self._bucket, self._name, self.upload_id, self.parts)
    self._client.complete_multipart_upload(request)
