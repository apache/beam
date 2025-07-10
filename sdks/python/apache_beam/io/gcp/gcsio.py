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

from google.api_core.exceptions import RetryError
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import from_http_response
from google.cloud.storage.fileio import BlobReader
from google.cloud.storage.fileio import BlobWriter
from google.cloud.storage.retry import DEFAULT_RETRY

from apache_beam import version as beam_version
from apache_beam.internal.gcp import auth
from apache_beam.io.gcp import gcsio_retry
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils.annotations import deprecated

__all__ = ['GcsIO', 'create_storage_client']

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


def create_storage_client(pipeline_options, use_credentials=True):
  """Create a GCS client for Beam via GCS Client Library.

  Args:
    pipeline_options(apache_beam.options.pipeline_options.PipelineOptions):
      the options of the pipeline.
    use_credentials(bool): whether to create an authenticated client based
      on pipeline options or an anonymous client.

  Returns:
    A google.cloud.storage.client.Client instance.
  """
  if use_credentials:
    credentials = auth.get_service_credentials(pipeline_options)
  else:
    credentials = None

  if credentials:
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    from google.api_core import client_info
    beam_client_info = client_info.ClientInfo(
        user_agent="apache-beam/%s (GPN:Beam)" % beam_version.__version__)

    extra_headers = {
        "x-goog-custom-audit-job": google_cloud_options.job_name
        if google_cloud_options.job_name else "UNKNOWN"
    }

    # Note: Custom audit entries with "job" key will overwrite the default above
    if google_cloud_options.gcs_custom_audit_entries is not None:
      extra_headers.update(google_cloud_options.gcs_custom_audit_entries)

    return storage.Client(
        credentials=credentials.get_google_auth_credentials(),
        project=google_cloud_options.project,
        client_info=beam_client_info,
        extra_headers=extra_headers)
  else:
    return storage.Client.create_anonymous_client()


class GcsIO(object):
  """Google Cloud Storage I/O client."""
  def __init__(
      self,
      storage_client: Optional[storage.Client] = None,
      pipeline_options: Optional[Union[dict, PipelineOptions]] = None) -> None:
    if pipeline_options is None:
      pipeline_options = PipelineOptions()
    elif isinstance(pipeline_options, dict):
      pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
    if storage_client is None:
      storage_client = create_storage_client(pipeline_options)

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    self.enable_read_bucket_metric = getattr(
        google_cloud_options, 'enable_bucket_read_metric_counter', False)
    self.enable_write_bucket_metric = getattr(
        google_cloud_options, 'enable_bucket_write_metric_counter', False)

    self.client = storage_client
    self._rewrite_cb = None
    self.bucket_to_project_number = {}
    self._storage_client_retry = gcsio_retry.get_retry(pipeline_options)
    self._use_blob_generation = getattr(
        google_cloud_options, 'enable_gcsio_blob_generation', False)

  def get_project_number(self, bucket):
    if bucket not in self.bucket_to_project_number:
      bucket_metadata = self.get_bucket(bucket_name=bucket)
      if bucket_metadata:
        self.bucket_to_project_number[bucket] = bucket_metadata.projectNumber

    return self.bucket_to_project_number.get(bucket, None)

  def get_bucket(self, bucket_name, **kwargs):
    """Returns an object bucket from its name, or None if it does not exist."""
    try:
      return self.client.lookup_bucket(
          bucket_name, retry=self._storage_client_retry, **kwargs)
    except NotFound:
      return None

  def create_bucket(
      self,
      bucket_name,
      project,
      kms_key=None,
      location=None,
      soft_delete_retention_duration_seconds=0):
    """Create and return a GCS bucket in a specific project."""

    try:
      bucket = self.client.bucket(bucket_name)
      bucket.soft_delete_policy.retention_duration_seconds = (
          soft_delete_retention_duration_seconds)
      bucket = self.client.create_bucket(
          bucket_or_name=bucket,
          project=project,
          location=location,
          retry=self._storage_client_retry)
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
    bucket = self.client.bucket(bucket_name)

    if mode == 'r' or mode == 'rb':
      blob = bucket.blob(blob_name)
      return BeamBlobReader(
          blob,
          chunk_size=read_buffer_size,
          enable_read_bucket_metric=self.enable_read_bucket_metric,
          retry=self._storage_client_retry)
    elif mode == 'w' or mode == 'wb':
      blob = bucket.blob(blob_name)
      return BeamBlobWriter(
          blob,
          mime_type,
          enable_write_bucket_metric=self.enable_write_bucket_metric,
          retry=self._storage_client_retry)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  def delete(self, path, recursive=False):
    """Deletes the object at the given GCS path.

    If the path is a directory (prefix), it deletes all blobs under that prefix
    when recursive=True.

    Args:
      path: GCS file path pattern in the form gs://<bucket>/<name>.
      recursive (bool, optional): If True, deletes all objects under the prefix
          when the path is a directory (default: False).
    """
    bucket_name, blob_name = parse_gcs_path(path)
    bucket = self.client.bucket(bucket_name)
    if recursive:
      # List all blobs under the prefix.
      blobs_to_delete = bucket.list_blobs(
          prefix=blob_name, retry=self._storage_client_retry)
      # Collect full paths for batch deletion.
      paths_to_delete = [
          f'gs://{bucket_name}/{blob.name}' for blob in blobs_to_delete
      ]
      if paths_to_delete:
        # Delete them in batches.
        results = self.delete_batch(paths_to_delete)
        # Log any errors encountered during batch deletion.
        errors = [f'{path}: {err}' for path, err in results if err is not None]
        if errors:
          _LOGGER.warning(
              'Failed to delete some objects during recursive delete of %s: %s',
              path,
              ', '.join(errors))
    else:
      # Delete only the specific blob.
      self._delete_blob(bucket, blob_name)

  def _delete_blob(self, bucket, blob_name):
    """Helper method to delete a single blob from GCS.

    Args:
      bucket: The GCS bucket object.
      blob_name: The name of the blob to delete under the bucket.
    """
    if self._use_blob_generation:
      # Fetch blob generation if required.
      blob = bucket.get_blob(blob_name, retry=self._storage_client_retry)
      generation = getattr(blob, "generation", None)
    else:
      generation = None
    try:
      bucket.delete_blob(
          blob_name,
          if_generation_match=generation,
          retry=self._storage_client_retry)
    except NotFound:
      return

  def _batch_with_retry(self, requests, fn):
    current_requests = [*enumerate(requests)]
    responses = [None for _ in current_requests]

    @self._storage_client_retry
    def run_with_retry():
      current_batch = self.client.batch(raise_exception=False)
      with current_batch:
        for _, request in current_requests:
          fn(request)
      last_retryable_exception = None
      for (i, current_pair), response in zip(
        [*current_requests], current_batch._responses
      ):
        responses[i] = response
        should_retry = (
            response.status_code >= 400 and
            self._storage_client_retry._predicate(from_http_response(response)))
        if should_retry:
          last_retryable_exception = from_http_response(response)
        else:
          current_requests.remove((i, current_pair))
      if last_retryable_exception:
        raise last_retryable_exception

    try:
      run_with_retry()
    except RetryError:
      pass

    return responses

  def _delete_batch_request(self, path):
    bucket_name, blob_name = parse_gcs_path(path)
    bucket = self.client.bucket(bucket_name)
    bucket.delete_blob(blob_name)

  def delete_batch(self, paths):
    """Deletes the objects at the given GCS paths.

    Args:
      paths: List of GCS file path patterns or Dict with GCS file path patterns
             as keys. The patterns are in the form gs://<bucket>/<name>, but
             not to exceed MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (path, exception) in the same order as the
             paths argument, where exception is None if the operation
             succeeded or the relevant exception if the operation failed.
    """
    final_results = []
    s = 0
    if not isinstance(paths, list): paths = list(iter(paths))
    while s < len(paths):
      if (s + MAX_BATCH_OPERATION_SIZE) < len(paths):
        current_paths = paths[s:s + MAX_BATCH_OPERATION_SIZE]
      else:
        current_paths = paths[s:]
      responses = self._batch_with_retry(
          current_paths, self._delete_batch_request)
      for i, path in enumerate(current_paths):
        error_code = None
        resp = responses[i]
        if resp.status_code >= 400 and resp.status_code != 404:
          error_code = resp.status_code
        final_results.append((path, error_code))

      s += MAX_BATCH_OPERATION_SIZE

    return final_results

  def copy(self, src, dest):
    """Copies the given GCS object from src to dest.

    Args:
      src: GCS file path pattern in the form gs://<bucket>/<name>.
      dest: GCS file path pattern in the form gs://<bucket>/<name>.

    Raises:
      Any exceptions during copying
    """
    src_bucket_name, src_blob_name = parse_gcs_path(src)
    dest_bucket_name, dest_blob_name= parse_gcs_path(dest, object_optional=True)
    src_bucket = self.client.bucket(src_bucket_name)
    if self._use_blob_generation:
      src_blob = src_bucket.get_blob(src_blob_name)
      if src_blob is None:
        raise NotFound("source blob %s not found during copying" % src)
      src_generation = src_blob.generation
    else:
      src_blob = src_bucket.blob(src_blob_name)
      src_generation = None
    dest_bucket = self.client.bucket(dest_bucket_name)
    if not dest_blob_name:
      dest_blob_name = None
    src_bucket.copy_blob(
        src_blob,
        dest_bucket,
        new_name=dest_blob_name,
        source_generation=src_generation,
        retry=self._storage_client_retry)

  def _copy_batch_request(self, pair):
    src_bucket_name, src_blob_name = parse_gcs_path(pair[0])
    dest_bucket_name, dest_blob_name = parse_gcs_path(pair[1])
    src_bucket = self.client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(src_blob_name)
    dest_bucket = self.client.bucket(dest_bucket_name)
    src_bucket.copy_blob(src_blob, dest_bucket, dest_blob_name)

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
      responses = self._batch_with_retry(
          current_pairs, self._copy_batch_request)
      for i, pair in enumerate(current_pairs):
        error_code = None
        resp = responses[i]
        if resp.status_code >= 400:
          error_code = resp.status_code
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
    bucket_name, blob_name = parse_gcs_path(path)
    bucket = self.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists(retry=self._storage_client_retry)

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

  def _gcs_object(self, path):
    """Returns a gcs object for the given path

    This method does not perform glob expansion. Hence the given path must be
    for a single GCS object. The method will make HTTP requests.

    Returns: GCS object.
    """
    bucket_name, blob_name = parse_gcs_path(path)
    bucket = self.client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name, retry=self._storage_client_retry)
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
    bucket = self.client.bucket(bucket_name)
    response = self.client.list_blobs(
        bucket, prefix=prefix, retry=self._storage_client_retry)
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

  def is_soft_delete_enabled(self, gcs_path):
    try:
      bucket_name, _ = parse_gcs_path(gcs_path)
      bucket = self.get_bucket(bucket_name)
      if (bucket.soft_delete_policy is not None and
          bucket.soft_delete_policy.retention_duration_seconds > 0):
        return True
    except Exception:
      _LOGGER.warning(
          "Unexpected error occurred when checking soft delete policy for %s" %
          gcs_path)
    return False


class BeamBlobReader(BlobReader):
  def __init__(
      self,
      blob,
      chunk_size=DEFAULT_READ_BUFFER_SIZE,
      enable_read_bucket_metric=False,
      retry=DEFAULT_RETRY,
      raw_download=True):
    # By default, we always request to retrieve raw data from GCS even if the
    # object meets the criteria of decompressive transcoding
    # (https://cloud.google.com/storage/docs/transcoding).
    super().__init__(
        blob, chunk_size=chunk_size, retry=retry, raw_download=raw_download)
    # TODO: Remove this after
    # https://github.com/googleapis/python-storage/issues/1406 is fixed.
    # As a workaround, we manually trigger a reload here. Otherwise, an internal
    # call of reader.seek() will cause an exception if raw_download is set
    # when initializing BlobReader(),
    blob.reload()

    # TODO: Currently there is a bug in GCS server side when a client requests
    # a file with "content-encoding=gzip" and "content-type=application/gzip" or
    # "content-type=application/x-gzip", which will lead to infinite loop.
    # We skip the support of this type of files until the GCS bug is fixed.
    # Internal bug id: 203845981.
    if (blob.content_encoding == "gzip" and
        blob.content_type in ["application/gzip", "application/x-gzip"]):
      raise NotImplementedError("Doubly compressed files not supported.")

    self.enable_read_bucket_metric = enable_read_bucket_metric
    self.mode = "r"

  def read(self, size=-1):
    bytesRead = super().read(size)
    if self.enable_read_bucket_metric:
      Metrics.counter(
          self.__class__,
          "GCS_read_bytes_counter_" + self._blob.bucket.name).inc(
              len(bytesRead))
    return bytesRead


class BeamBlobWriter(BlobWriter):
  def __init__(
      self,
      blob,
      content_type,
      chunk_size=16 * 1024 * 1024,
      ignore_flush=True,
      enable_write_bucket_metric=False,
      retry=DEFAULT_RETRY):
    super().__init__(
        blob,
        content_type=content_type,
        chunk_size=chunk_size,
        ignore_flush=ignore_flush,
        retry=retry)
    self.mode = "w"
    self.enable_write_bucket_metric = enable_write_bucket_metric

  def write(self, b):
    bytesWritten = super().write(b)
    if self.enable_write_bucket_metric:
      Metrics.counter(
          self.__class__, "GCS_write_bytes_counter_" +
          self._blob.bucket.name).inc(bytesWritten)
    return bytesWritten
