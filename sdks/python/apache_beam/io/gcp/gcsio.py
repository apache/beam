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

**Updates to the I/O connector code**

For any significant updates to this I/O connector, please consider involving
corresponding code reviewers mentioned in
https://github.com/apache/beam/blob/master/sdks/python/OWNERS
"""

# pytype: skip-file

import logging
import re
import time
from typing import Optional
from typing import Union

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage.fileio import BlobReader
from google.cloud.storage.fileio import BlobWriter

from apache_beam.internal.gcp import auth
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import retry
from apache_beam.utils.annotations import deprecated

__all__ = ['GcsIO']

_LOGGER = logging.getLogger(__name__)

DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024

# Maximum number of operations permitted in GcsIO.copy_batch() and
# GcsIO.delete_batch().
MAX_BATCH_OPERATION_SIZE = 100


def parse_gcs_path(gcs_path, object_optional=False):
  """Return the bucket and object names of the given gs:// path."""
  match = re.match('^gs://([^/]+)/(.*)$', gcs_path)
  if match is None or (match.group(2) == '' and not object_optional):
    raise ValueError(
        'GCS path must be in the form gs://<bucket>/<object>. '
        f'Encountered {gcs_path!r}')
  return match.group(1), match.group(2)


def default_gcs_bucket_name(project, region):
  from hashlib import md5
  return 'dataflow-staging-%s-%s' % (
      region, md5(project.encode('utf8')).hexdigest())


def get_or_create_default_gcs_bucket(options):
  """Create a default GCS bucket for this project."""
  if getattr(options, 'dataflow_kms_key', None):
    _LOGGER.warning(
        'Cannot create a default bucket when --dataflow_kms_key is set.')
    return None

  project = getattr(options, 'project', None)
  region = getattr(options, 'region', None)
  if not project or not region:
    return None

  bucket_name = default_gcs_bucket_name(project, region)
  bucket = GcsIO(pipeline_options=options).get_bucket(bucket_name)
  if bucket:
    return bucket
  else:
    _LOGGER.warning(
        'Creating default GCS bucket for project %s: gs://%s',
        project,
        bucket_name)
    return GcsIO(pipeline_options=options).create_bucket(
        bucket_name, project, location=region)


class GcsIO(object):
  """Google Cloud Storage I/O client."""
  def __init__(self, storage_client=None, pipeline_options=None):
    # type: (Optional[storage.Client], Optional[Union[dict, PipelineOptions]]) -> None
    if storage_client is None:
      if not pipeline_options:
        pipeline_options = PipelineOptions()
      elif isinstance(pipeline_options, dict):
        pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
      credentials = auth.get_service_credentials(pipeline_options)
      if credentials:
        storage_client = storage.Client(
            credentials=credentials.get_google_auth_credentials(),
            project=pipeline_options.view_as(GoogleCloudOptions).project)
      else:
        storage_client = storage.Client.create_anonymous_client()
    self.client = storage_client
    self._rewrite_cb = None
    self.bucket_to_project_number = {}

  def get_project_number(self, bucket):
    if bucket not in self.bucket_to_project_number:
      bucket_metadata = self.get_bucket(bucket_name=bucket)
      if bucket_metadata:
        self.bucket_to_project_number[bucket] = bucket_metadata.projectNumber

    return self.bucket_to_project_number.get(bucket, None)

  def get_bucket(self, bucket_name):
    """Returns an object bucket from its name, or None if it does not exist."""
    try:
      return self.client.lookup_bucket(bucket_name)
    except NotFound:
      return None

  def create_bucket(self, bucket_name, project, kms_key=None, location=None):
    """Create and return a GCS bucket in a specific project."""

    try:
      bucket = self.client.create_bucket(
          bucket_or_name=bucket_name,
          project=project,
          location=location,
      )
      if kms_key:
        bucket.default_kms_key_name(kms_key)
        bucket.patch()
      return bucket
    except NotFound:
      return None

  def open(
      self,
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
      ValueError: Invalid open file mode.
    """
    bucket_name, blob_name = parse_gcs_path(filename)
    bucket = self.client.get_bucket(bucket_name)

    if mode == 'r' or mode == 'rb':
      blob = bucket.get_blob(blob_name)
      return BeamBlobReader(blob, chunk_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      blob = bucket.get_blob(blob_name)
      if not blob:
        blob = storage.Blob(blob_name, bucket)
      return BeamBlobWriter(blob, mime_type)

    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def delete(self, path):
    """Deletes the object at the given GCS path.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    bucket_name, blob_name = parse_gcs_path(path)
    try:
      bucket = self.client.get_bucket(bucket_name)
      bucket.delete_blob(blob_name)
    except NotFound:
      return

  def delete_batch(self, paths):
    """Deletes the objects at the given GCS paths.

    Args:
      paths: List of GCS file path patterns in the form gs://<bucket>/<name>,
             not to exceed MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (path, exception) in the same order as the
             paths argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    final_results = []
    s = 0
    while s < len(paths):
      if (s + MAX_BATCH_OPERATION_SIZE) < len(paths):
        current_paths = paths[s:s + MAX_BATCH_OPERATION_SIZE]
      else:
        current_paths = paths[s:]
      current_batch = self.client.batch(raise_exception=False)
      with current_batch:
        for path in current_paths:
          bucket_name, blob_name = parse_gcs_path(path)
          bucket = self.client.get_bucket(bucket_name)
          bucket.delete_blob(blob_name)

      for i, path in enumerate(current_paths):
        error_code = None
        for j in range(2):
          resp = current_batch._responses[2 * i + j]
          if resp.status_code >= 400 and resp.status_code != 404:
            error_code = resp.status_code
            break
        final_results.append((path, error_code))

      s += MAX_BATCH_OPERATION_SIZE

    return final_results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def copy(self, src, dest):
    """Copies the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.

    Raises:
      TimeoutError: on timeout.
    """
    src_bucket_name, src_blob_name = parse_gcs_path(src)
    dest_bucket_name, dest_blob_name= parse_gcs_path(dest, object_optional=True)
    src_bucket = self.get_bucket(src_bucket_name)
    src_blob = src_bucket.get_blob(src_blob_name)
    if not src_blob:
      raise NotFound("Source %s not found", src)
    dest_bucket = self.get_bucket(dest_bucket_name)
    if not dest_blob_name:
      dest_blob_name = None
    src_bucket.copy_blob(src_blob, dest_bucket, new_name=dest_blob_name)

  def copy_batch(self, src_dest_pairs):
    """Copies the given GCS objects from src to dest.

    Args:
      src_dest_pairs: list of (src, dest) tuples of gs://<bucket>/<name> files
                      paths to copy from src to dest, not to exceed
                      MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (src, dest, exception) in the same order as the
             src_dest_pairs argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    final_results = []
    s = 0
    while s < len(src_dest_pairs):
      if (s + MAX_BATCH_OPERATION_SIZE) < len(src_dest_pairs):
        current_pairs = src_dest_pairs[s:s + MAX_BATCH_OPERATION_SIZE]
      else:
        current_pairs = src_dest_pairs[s:]
      current_batch = self.client.batch(raise_exception=False)
      with current_batch:
        for pair in current_pairs:
          src_bucket_name, src_blob_name = parse_gcs_path(pair[0])
          dest_bucket_name, dest_blob_name = parse_gcs_path(pair[1])
          src_bucket = self.client.get_bucket(src_bucket_name)
          src_blob = src_bucket.get_blob(src_blob_name)
          dest_bucket = self.client.get_bucket(dest_bucket_name)

          src_bucket.copy_blob(src_blob, dest_bucket, dest_blob_name)

      for i, pair in enumerate(current_pairs):
        error_code = None
        for j in range(4):
          resp = current_batch._responses[4 * i + j]
          if resp.status_code >= 400:
            error_code = resp.status_code
            break
        final_results.append((pair[0], pair[1], error_code))

      s += MAX_BATCH_OPERATION_SIZE

    return final_results

  # We intentionally do not decorate this method with a retry, since the
  # underlying copy and delete operations are already idempotent operations.
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
  # underlying copy and delete operations are already idempotent operations.
  def rename(self, src, dest):
    """Renames the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    self.copy(src, dest)
    self.delete(src)

  def exists(self, path):
    """Returns whether the given GCS object exists.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    try:
      self._gcs_object(path)
      return True
    except NotFound:
      return False

  def checksum(self, path):
    """Looks up the checksum of a GCS object.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
    """
    return self._gcs_object(path).crc32c

  def size(self, path):
    """Returns the size of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: size of the GCS object in bytes.
    """
    return self._gcs_object(path).size

  def kms_key(self, path):
    """Returns the KMS key of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: KMS key name of the GCS object as a string, or None if it doesn't
      have one.
    """
    return self._gcs_object(path).kms_key_name

  def last_updated(self, path):
    """Returns the last updated epoch time of a single GCS object.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: last updated time of the GCS object in second.
    """
    return self._updated_to_seconds(self._gcs_object(path).updated)

  def _status(self, path):
    """For internal use only; no backwards-compatibility guarantees.

    Returns supported fields (checksum, kms_key, last_updated, size) of a
    single object as a dict at once.

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: dict of fields of the GCS object.
    """
    gcs_object = self._gcs_object(path)
    file_status = {}
    if hasattr(gcs_object, 'crc32c'):
      file_status['checksum'] = gcs_object.crc32c
    if hasattr(gcs_object, 'kms_key_name'):
      file_status['kms_key'] = gcs_object.kms_key_name
    if hasattr(gcs_object, 'updated'):
      file_status['updated'] = self._updated_to_seconds(gcs_object.updated)
    if hasattr(gcs_object, 'size'):
      file_status['size'] = gcs_object.size
    return file_status

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _gcs_object(self, path):
    """Returns a gcs object for the given path

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object.

    Returns: GCS object.
    """
    bucket_name, blob_name = parse_gcs_path(path)
    bucket = self.client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    if blob:
      return blob
    else:
      raise NotFound('Object %s not found', path)

  @deprecated(since='2.45.0', current='list_files')
  def list_prefix(self, path, with_metadata=False):
    """Lists files matching the prefix.

    ``list_prefix`` has been deprecated. Use `list_files` instead, which returns
    a generator of file information instead of a dict.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/[name].
      with_metadata: Experimental. Specify whether returns file metadata.

    Returns:
      If ``with_metadata`` is False: dict of file name -> size; if
        ``with_metadata`` is True: dict of file name -> tuple(size, timestamp).
    """
    file_info = {}
    for file_metadata in self.list_files(path, with_metadata):
      file_info[file_metadata[0]] = file_metadata[1]

    return file_info

  def list_files(self, path, with_metadata=False):
    """Lists files matching the prefix.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/[name].
      with_metadata: Experimental. Specify whether returns file metadata.

    Returns:
      If ``with_metadata`` is False: generator of tuple(file name, size); if
      ``with_metadata`` is True: generator of
      tuple(file name, tuple(size, timestamp)).
    """
    bucket_name, prefix = parse_gcs_path(path, object_optional=True)
    file_info = set()
    counter = 0
    start_time = time.time()
    if with_metadata:
      _LOGGER.debug("Starting the file information of the input")
    else:
      _LOGGER.debug("Starting the size estimation of the input")
    bucket = self.client.get_bucket(bucket_name)
    response = self.client.list_blobs(bucket, prefix=prefix)
    for item in response:
      file_name = 'gs://%s/%s' % (item.bucket.name, item.name)
      if file_name not in file_info:
        file_info.add(file_name)
        counter += 1
        if counter % 10000 == 0:
          if with_metadata:
            _LOGGER.info(
                "Finished computing file information of: %s files",
                len(file_info))
          else:
            _LOGGER.info("Finished computing size of: %s files", len(file_info))

        if with_metadata:
          yield file_name, (item.size, self._updated_to_seconds(item.updated))
        else:
          yield file_name, item.size

    _LOGGER.log(
        # do not spam logs when list_prefix is likely used to check empty folder
        logging.INFO if counter > 0 else logging.DEBUG,
        "Finished listing %s files in %s seconds.",
        counter,
        time.time() - start_time)

  @staticmethod
  def _updated_to_seconds(updated):
    """Helper function transform the updated field of response to seconds"""
    return (
        time.mktime(updated.timetuple()) - time.timezone +
        updated.microsecond / 1000000.0)


class BeamBlobReader(BlobReader):
  def __init__(self, blob, chunk_size=DEFAULT_READ_BUFFER_SIZE):
    super().__init__(blob, chunk_size=chunk_size)
    self.mode = "r"


class BeamBlobWriter(BlobWriter):
  def __init__(
      self, blob, content_type, chunk_size=16 * 1024 * 1024, ignore_flush=True):
    super().__init__(
        blob,
        content_type=content_type,
        chunk_size=chunk_size,
        ignore_flush=ignore_flush)
    self.mode = "w"
