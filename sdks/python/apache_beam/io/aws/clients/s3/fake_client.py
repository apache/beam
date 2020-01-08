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

from __future__ import absolute_import

import datetime
import time

from apache_beam.io.aws.clients.s3 import messages


class FakeFile(object):

  def __init__(self, bucket, key, contents, etag=None):
    self.bucket = bucket
    self.key = key
    self.contents = contents

    self.last_modified = time.time()

    if not etag:
      self.etag = '"%s-1"' % ('x' * 32)
    else:
      self.etag = etag

  def get_metadata(self):
    last_modified_datetime = None
    if self.last_modified:
      last_modified_datetime = datetime.datetime.utcfromtimestamp(
          self.last_modified)

    return messages.Item(self.etag,
                         self.key,
                         last_modified_datetime,
                         len(self.contents),
                         mime_type=None)


class FakeS3Client(object):
  def __init__(self):
    self.files = {}
    self.list_continuation_tokens = {}
    self.multipart_uploads = {}

    # boto3 has different behavior when running some operations against a bucket
    # that exists vs. against one that doesn't. To emulate that behavior, the
    # mock client keeps a set of bucket names that it knows "exist".
    self.known_buckets = set()

  def add_file(self, f):
    self.files[(f.bucket, f.key)] = f
    if f.bucket not in self.known_buckets:
      self.known_buckets.add(f.bucket)

  def get_file(self, bucket, obj):
    try:
      return self.files[bucket, obj]
    except:
      raise messages.S3ClientError('Not Found', 404)

  def delete_file(self, bucket, obj):
    del self.files[(bucket, obj)]

  def get_object_metadata(self, request):
    r"""Retrieves an object's metadata.

    Args:
      request: (GetRequest) input message

    Returns:
      (Item) The response message.
    """
    # TODO: Do we want to mock out a lack of credentials?
    file_ = self.get_file(request.bucket, request.object)
    return file_.get_metadata()

  def list(self, request):
    bucket = request.bucket
    prefix = request.prefix or ''
    matching_files = []

    for file_bucket, file_name in sorted(iter(self.files)):
      if bucket == file_bucket and file_name.startswith(prefix):
        file_object = self.get_file(file_bucket, file_name).get_metadata()
        matching_files.append(file_object)

    if not matching_files:
      message = 'Tried to list nonexistent S3 path: s3://%s/%s' % (
          bucket, prefix)
      raise messages.S3ClientError(message, 404)

    # Handle pagination.
    items_per_page = 5
    if not request.continuation_token:
      range_start = 0
    else:
      if request.continuation_token not in self.list_continuation_tokens:
        raise ValueError('Invalid page token.')
      range_start = self.list_continuation_tokens[request.continuation_token]
      del self.list_continuation_tokens[request.continuation_token]

    result = messages.ListResponse(
        items=matching_files[range_start:range_start + items_per_page])

    if range_start + items_per_page < len(matching_files):
      next_range_start = range_start + items_per_page
      next_continuation_token = '_page_token_%s_%s_%d' % (bucket, prefix,
                                                          next_range_start)
      self.list_continuation_tokens[next_continuation_token] = next_range_start
      result.next_token = next_continuation_token

    return result

  def get_range(self, request, start, end):
    r"""Retrieves an object.

      Args:
        request: (GetRequest) request
      Returns:
        (bytes) The response message.
      """

    file_ = self.get_file(request.bucket, request.object)

    # Replicates S3's behavior, per the spec here:
    # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    if start < 0 or end <= start:
      return file_.contents

    return file_.contents[start:end]

  def delete(self, request):
    if request.bucket not in self.known_buckets:
      raise messages.S3ClientError('The specified bucket does not exist', 404)

    if (request.bucket, request.object) in self.files:
      self.delete_file(request.bucket, request.object)
    else:
      # S3 doesn't raise an error if you try to delete a nonexistent file from
      # an extant bucket
      return

  def delete_batch(self, request):

    deleted, failed, errors = [], [], []
    for object in request.objects:
      try:
        delete_request = messages.DeleteRequest(request.bucket, object)
        self.delete(delete_request)
        deleted.append(object)
      except messages.S3ClientError as e:
        failed.append(object)
        errors.append(e)

    return messages.DeleteBatchResponse(deleted, failed, errors)

  def copy(self, request):

    src_file = self.get_file(request.src_bucket, request.src_key)
    dest_file = FakeFile(request.dest_bucket,
                         request.dest_key,
                         src_file.contents)
    self.add_file(dest_file)

  def create_multipart_upload(self, request):
    # Create hash of bucket and key
    # Store upload_id internally
    upload_id = request.bucket + request.object
    self.multipart_uploads[upload_id] = {}
    return messages.UploadResponse(upload_id)

  def upload_part(self, request):
    # Save off bytes passed to internal data store
    upload_id, part_number = request.upload_id, request.part_number

    if part_number < 0 or not isinstance(part_number, int):
      raise messages.S3ClientError('Param validation failed on part number',
                                   400)

    if upload_id not in self.multipart_uploads:
      raise messages.S3ClientError('The specified upload does not exist', 404)

    self.multipart_uploads[upload_id][part_number] = request.bytes

    etag = '"%s"' % ('x' * 32)
    return messages.UploadPartResponse(etag, part_number)

  def complete_multipart_upload(self, request):
    MIN_PART_SIZE = 5 * 2**10 # 5 KiB

    parts_received = self.multipart_uploads[request.upload_id]

    # Check that we got all the parts that they intended to send
    part_numbers_to_confirm = set(part['PartNumber'] for part in request.parts)

    # Make sure all the expected parts are present
    if part_numbers_to_confirm != set(parts_received.keys()):
      raise messages.S3ClientError(
          'One or more of the specified parts could not be found', 400)

    # Sort by part number
    sorted_parts = sorted(parts_received.items(), key=lambda pair: pair[0])
    sorted_bytes = [bytes_ for (_, bytes_) in sorted_parts]

    # Make sure that the parts aren't too small (except the last part)
    part_sizes = [len(bytes_) for bytes_ in sorted_bytes]
    if any(size < MIN_PART_SIZE for size in part_sizes[:-1]):
      e_message = """
      All parts but the last must be larger than %d bytes
      """ % MIN_PART_SIZE
      raise messages.S3ClientError(e_message, 400)

    # String together all bytes for the given upload
    final_contents = b''.join(sorted_bytes)

    # Create FakeFile object
    num_parts = len(parts_received)
    etag = '"%s-%d"' % ('x' * 32, num_parts)
    file_ = FakeFile(request.bucket, request.object, final_contents, etag=etag)

    # Store FakeFile in self.files
    self.add_file(file_)
