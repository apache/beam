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

"""Azure Blob Storage client.
"""

# pytype: skip-file

import errno
import io
import logging
import os
import re
import tempfile
import time

from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystemio import Uploader
from apache_beam.io.filesystemio import UploaderStream
from apache_beam.utils import retry

_LOGGER = logging.getLogger(__name__)

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  from azure.core.exceptions import ResourceNotFoundError
  from azure.storage.blob import (
      BlobServiceClient,
      ContentSettings,
  )
  AZURE_DEPS_INSTALLED = True
except ImportError:
  AZURE_DEPS_INSTALLED = False

DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024

MAX_BATCH_OPERATION_SIZE = 100


def parse_azfs_path(azfs_path, blob_optional=False, get_account=False):
  """Return the storage account, the container and
  blob names of the given azfs:// path.
  """
  match = re.match(
      '^azfs://([a-z0-9]{3,24})/([a-z0-9](?![a-z0-9-]*--[a-z0-9-]*)'
      '[a-z0-9-]{1,61}[a-z0-9])/(.*)$',
      azfs_path)
  if match is None or (match.group(3) == '' and not blob_optional):
    raise ValueError(
        'Azure Blob Storage path must be in the form '
        'azfs://<storage-account>/<container>/<path>.')
  result = None
  if get_account:
    result = match.group(1), match.group(2), match.group(3)
  else:
    result = match.group(2), match.group(3)
  return result


def get_azfs_url(storage_account, container, blob=''):
  """Returns the url in the form of
   https://account.blob.core.windows.net/container/blob-name
  """
  return 'https://' + storage_account + '.blob.core.windows.net/' + \
          container + '/' + blob


class Blob():
  """A Blob in Azure Blob Storage."""
  def __init__(self, etag, name, last_updated, size, mime_type):
    self.etag = etag
    self.name = name
    self.last_updated = last_updated
    self.size = size
    self.mime_type = mime_type


class BlobStorageIOError(IOError, retry.PermanentException):
  """Blob Strorage IO error that should not be retried."""
  pass


class BlobStorageError(Exception):
  """Blob Storage client error."""
  def __init__(self, message=None, code=None):
    self.message = message
    self.code = code


class BlobStorageIO(object):
  """Azure Blob Storage I/O client."""
  def __init__(self, client=None):
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    if client is None:
      self.client = BlobServiceClient.from_connection_string(connect_str)
    else:
      self.client = client
    if not AZURE_DEPS_INSTALLED:
      raise RuntimeError('Azure dependencies are not installed. Unable to run.')

  def open(
      self,
      filename,
      mode='r',
      read_buffer_size=DEFAULT_READ_BUFFER_SIZE,
      mime_type='application/octet-stream'):
    """Open an Azure Blob Storage file path for reading or writing.

    Args:
      filename (str): Azure Blob Storage file path in the form
                      ``azfs://<storage-account>/<container>/<path>``.
      mode (str): ``'r'`` for reading or ``'w'`` for writing.
      read_buffer_size (int): Buffer size to use during read operations.
      mime_type (str): Mime type to set for write operations.

    Returns:
      Azure Blob Storage file object.
    Raises:
      ValueError: Invalid open file mode.
    """
    if mode == 'r' or mode == 'rb':
      downloader = BlobStorageDownloader(
          self.client, filename, buffer_size=read_buffer_size)
      return io.BufferedReader(
          DownloaderStream(
              downloader, read_buffer_size=read_buffer_size, mode=mode),
          buffer_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      uploader = BlobStorageUploader(self.client, filename, mime_type)
      return io.BufferedWriter(
          UploaderStream(uploader, mode=mode), buffer_size=128 * 1024)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def copy(self, src, dest):
    """Copies a single Azure Blob Storage blob from src to dest.

    Args:
      src: Blob Storage file path pattern in the form
           azfs://<storage-account>/<container>/[name].
      dest: Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].

    Raises:
      TimeoutError: on timeout.
    """
    src_storage_account, src_container, src_blob = parse_azfs_path(
        src, get_account=True)
    dest_container, dest_blob = parse_azfs_path(dest)

    source_blob = get_azfs_url(src_storage_account, src_container, src_blob)
    copied_blob = self.client.get_blob_client(dest_container, dest_blob)

    try:
      copied_blob.start_copy_from_url(source_blob)
    except ResourceNotFoundError as e:
      message = e.reason
      code = e.status_code
      raise BlobStorageError(message, code)

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy operation is already an idempotent operation protected
  # by retry decorators.
  def copy_tree(self, src, dest):
    """Renames the given Azure Blob storage directory and its contents
    recursively from src to dest.

    Args:
      src: Blob Storage file path pattern in the form
           azfs://<storage-account>/<container>/[name].
      dest: Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].

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
      except BlobStorageError as e:
        results.append((entry, dest + rel_path, e))

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy operation is already an idempotent operation protected
  # by retry decorators.
  def copy_paths(self, src_dest_pairs):
    """Copies the given Azure Blob Storage blobs from src to dest. This can
    handle directory or file paths.

    Args:
      src_dest_pairs: List of (src, dest) tuples of
                      azfs://<storage-account>/<container>/[name] file paths
                      to copy from src to dest.

    Returns:
      List of tuples of (src, dest, exception) in the same order as the
      src_dest_pairs argument, where exception is None if the operation
      succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs:
      return []

    results = []

    for src_path, dest_path in src_dest_pairs:
      # Case 1. They are directories.
      if src_path.endswith('/') and dest_path.endswith('/'):
        try:
          results += self.copy_tree(src_path, dest_path)
        except BlobStorageError as e:
          results.append((src_path, dest_path, e))

      # Case 2. They are individual blobs.
      elif not src_path.endswith('/') and not dest_path.endswith('/'):
        try:
          self.copy(src_path, dest_path)
          results.append((src_path, dest_path, None))
        except BlobStorageError as e:
          results.append((src_path, dest_path, e))

      # Mismatched paths (one directory, one non-directory) get an error.
      else:
        e = BlobStorageError(
            "Unable to copy mismatched paths" +
            "(directory, non-directory): %s, %s" % (src_path, dest_path),
            400)
        results.append((src_path, dest_path, e))

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def rename(self, src, dest):
    """Renames the given Azure Blob Storage blob from src to dest.

    Args:
      src: Blob Storage file path pattern in the form
           azfs://<storage-account>/<container>/[name].
      dest: Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].
    """
    self.copy(src, dest)
    self.delete(src)

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def rename_files(self, src_dest_pairs):
    """Renames the given Azure Blob Storage blobs from src to dest.

    Args:
      src_dest_pairs: List of (src, dest) tuples of
                      azfs://<storage-account>/<container>/[name]
                      file paths to rename from src to dest.
    Returns: List of tuples of (src, dest, exception) in the same order as the
             src_dest_pairs argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    if not src_dest_pairs:
      return []

    for src, dest in src_dest_pairs:
      if src.endswith('/') or dest.endswith('/'):
        raise ValueError('Unable to rename a directory.')

    # Results from copy operation.
    copy_results = self.copy_paths(src_dest_pairs)
    paths_to_delete = \
        [src for (src, _, error) in copy_results if error is None]
    # Results from delete operation.
    delete_results = self.delete_files(paths_to_delete)

    # Get rename file results (list of tuples).
    results = []

    # Using a dictionary will make the operation faster.
    delete_results_dict = {src: error for (src, error) in delete_results}

    for src, dest, error in copy_results:
      # If there was an error in the copy operation.
      if error is not None:
        results.append((src, dest, error))
      # If there was an error in the delete operation.
      elif delete_results_dict[src] is not None:
        results.append((src, dest, delete_results_dict[src]))
      # If there was no error in the operations.
      else:
        results.append((src, dest, None))

    return results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def exists(self, path):
    """Returns whether the given Azure Blob Storage blob exists.

    Args:
      path: Azure Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].
    """
    container, blob = parse_azfs_path(path)
    blob_to_check = self.client.get_blob_client(container, blob)
    try:
      blob_to_check.get_blob_properties()
      return True
    except ResourceNotFoundError as e:
      if e.status_code == 404:
        # HTTP 404 indicates that the file did not exist.
        return False
      else:
        # We re-raise all other exceptions.
        raise

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def size(self, path):
    """Returns the size of a single Blob Storage blob.

    This method does not perform glob expansion. Hence the
    given path must be for a single Blob Storage blob.

    Returns: size of the Blob Storage blob in bytes.
    """
    container, blob = parse_azfs_path(path)
    blob_to_check = self.client.get_blob_client(container, blob)
    try:
      properties = blob_to_check.get_blob_properties()
    except ResourceNotFoundError as e:
      message = e.reason
      code = e.status_code
      raise BlobStorageError(message, code)

    return properties.size

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def last_updated(self, path):
    """Returns the last updated epoch time of a single
    Azure Blob Storage blob.

    This method does not perform glob expansion. Hence the
    given path must be for a single Azure Blob Storage blob.

    Returns: last updated time of the Azure Blob Storage blob
    in seconds.
    """
    container, blob = parse_azfs_path(path)
    blob_to_check = self.client.get_blob_client(container, blob)
    try:
      properties = blob_to_check.get_blob_properties()
    except ResourceNotFoundError as e:
      message = e.reason
      code = e.status_code
      raise BlobStorageError(message, code)

    datatime = properties.last_modified
    return (
        time.mktime(datatime.timetuple()) - time.timezone +
        datatime.microsecond / 1000000.0)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def checksum(self, path):
    """Looks up the checksum of an Azure Blob Storage blob.

    Args:
      path: Azure Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].
    """
    container, blob = parse_azfs_path(path)
    blob_to_check = self.client.get_blob_client(container, blob)
    try:
      properties = blob_to_check.get_blob_properties()
    except ResourceNotFoundError as e:
      message = e.reason
      code = e.status_code
      raise BlobStorageError(message, code)

    return properties.etag

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def delete(self, path):
    """Deletes a single blob at the given Azure Blob Storage path.

    Args:
      path: Azure Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].
    """
    container, blob = parse_azfs_path(path)
    blob_to_delete = self.client.get_blob_client(container, blob)
    try:
      blob_to_delete.delete_blob()
    except ResourceNotFoundError as e:
      if e.status_code == 404:
        # Return success when the file doesn't exist anymore for idempotency.
        return
      else:
        logging.error('HTTP error while deleting file %s', path)
        raise e

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_paths(self, paths):
    """Deletes the given Azure Blob Storage blobs from src to dest.
    This can handle directory or file paths.

    Args:
      paths: list of Azure Blob Storage paths in the form
             azfs://<storage-account>/<container>/[name] that give the
             file blobs to be deleted.

    Returns:
      List of tuples of (src, dest, exception) in the same order as the
      src_dest_pairs argument, where exception is None if the operation
      succeeded or the relevant exception if the operation failed.
    """
    directories, blobs = [], []

    # Retrieve directories and not directories.
    for path in paths:
      if path.endswith('/'):
        directories.append(path)
      else:
        blobs.append(path)

    results = {}

    for directory in directories:
      directory_result = dict(self.delete_tree(directory))
      results.update(directory_result)

    blobs_results = dict(self.delete_files(blobs))
    results.update(blobs_results)

    return results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_tree(self, root):
    """Deletes all blobs under the given Azure BlobStorage virtual
    directory.

    Args:
      path: Azure Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name]
            (ending with a "/").

    Returns:
      List of tuples of (path, exception), where each path is a blob
      under the given root. exception is None if the operation succeeded
      or the relevant exception if the operation failed.
    """
    assert root.endswith('/')

    # Get the blob under the root directory.
    paths_to_delete = self.list_prefix(root)

    return self.delete_files(paths_to_delete)

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations
  # protected by retry decorators.
  def delete_files(self, paths):
    """Deletes the given Azure Blob Storage blobs from src to dest.

    Args:
      paths: list of Azure Blob Storage paths in the form
             azfs://<storage-account>/<container>/[name] that give the
             file blobs to be deleted.

    Returns:
      List of tuples of (src, dest, exception) in the same order as the
      src_dest_pairs argument, where exception is None if the operation
      succeeded or the relevant exception if the operation failed.
    """
    if not paths:
      return []

    # Group blobs into containers.
    containers, blobs = zip(*[parse_azfs_path(path, get_account=False) \
        for path in paths])

    grouped_blobs = {container: [] for container in containers}

    # Fill dictionary.
    for container, blob in zip(containers, blobs):
      grouped_blobs[container].append(blob)

    results = {}

    # Delete minibatches of blobs for each container.
    for container, blobs in grouped_blobs.items():
      for i in range(0, len(blobs), MAX_BATCH_OPERATION_SIZE):
        blobs_to_delete = blobs[i:i + MAX_BATCH_OPERATION_SIZE]
        results.update(self._delete_batch(container, blobs_to_delete))

    final_results = \
        [(path, results[parse_azfs_path(path, get_account=False)]) \
        for path in paths]

    return final_results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def _delete_batch(self, container, blobs):
    """A helper method. Azure Blob Storage Python Client allows batch
    deletions for blobs within the same container.

    Args:
      container: container name.
      blobs: list of blobs to be deleted.

    Returns:
      Dictionary of the form {(container, blob): error}, where error is
      None if the operation succeeded.
    """
    container_client = self.client.get_container_client(container)
    results = {}

    for blob in blobs:
      try:
        response = container_client.delete_blob(blob)
        results[(container, blob)] = response
      except ResourceNotFoundError as e:
        results[(container, blob)] = e.status_code

    return results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def list_prefix(self, path):
    """Lists files matching the prefix.

    Args:
      path: Azure Blob Storage file path pattern in the form
            azfs://<storage-account>/<container>/[name].

    Returns:
      Dictionary of file name -> size.
    """
    storage_account, container, blob = parse_azfs_path(
        path, blob_optional=True, get_account=True)
    file_sizes = {}
    counter = 0
    start_time = time.time()

    logging.info("Starting the size estimation of the input")
    container_client = self.client.get_container_client(container)

    while True:
      response = container_client.list_blobs(name_starts_with=blob)
      for item in response:
        file_name = "azfs://%s/%s/%s" % (storage_account, container, item.name)
        file_sizes[file_name] = item.size
        counter += 1
        if counter % 10000 == 0:
          logging.info("Finished computing size of: %s files", len(file_sizes))
      break

    logging.info(
        "Finished listing %s files in %s seconds.",
        counter,
        time.time() - start_time)
    return file_sizes


class BlobStorageDownloader(Downloader):
  def __init__(self, client, path, buffer_size):
    self._client = client
    self._path = path
    self._container, self._blob = parse_azfs_path(path)
    self._buffer_size = buffer_size

    self._blob_to_download = self._client.get_blob_client(
        self._container, self._blob)

    try:
      properties = self._get_object_properties()
    except ResourceNotFoundError as http_error:
      if http_error.status_code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        _LOGGER.error(
            'HTTP error while requesting file %s: %s', self._path, http_error)
        raise

    self._size = properties.size

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_beam_io_error_filter)
  def _get_object_properties(self):
    return self._blob_to_download.get_blob_properties()

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    # Download_blob first parameter is offset and second is length (exclusive).
    blob_data = self._blob_to_download.download_blob(start, end - start)
    # Returns the content as bytes.
    return blob_data.readall()


class BlobStorageUploader(Uploader):
  def __init__(self, client, path, mime_type='application/octet-stream'):
    self._client = client
    self._path = path
    self._container, self._blob = parse_azfs_path(path)
    self._content_settings = ContentSettings(mime_type)

    self._blob_to_upload = self._client.get_blob_client(
        self._container, self._blob)

    # Temporary file.
    self._temporary_file = tempfile.NamedTemporaryFile()

  def put(self, data):
    self._temporary_file.write(data.tobytes())

  def finish(self):
    self._temporary_file.seek(0)
    # The temporary file is deleted immediately after the operation.
    with open(self._temporary_file.name, "rb") as f:
      self._blob_to_upload.upload_blob(
          f.read(), overwrite=True, content_settings=self._content_settings)
