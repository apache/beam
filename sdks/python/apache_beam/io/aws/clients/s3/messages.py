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

# pytype: skip-file


class GetRequest():
  """
  S3 request object for `Get` command
  """
  def __init__(self, bucket, object):
    self.bucket = bucket
    self.object = object


class UploadResponse():
  """
  S3 response object for `StartUpload` command
  """
  def __init__(self, upload_id):
    self.upload_id = upload_id


class UploadRequest():
  """
  S3 request object for `StartUpload` command
  """
  def __init__(self, bucket, object, mime_type):
    self.bucket = bucket
    self.object = object
    self.mime_type = mime_type


class UploadPartRequest():
  """
  S3 request object for `UploadPart` command
  """
  def __init__(self, bucket, object, upload_id, part_number, bytes):
    self.bucket = bucket
    self.object = object
    self.upload_id = upload_id
    self.part_number = part_number
    self.bytes = bytes
    # self.mime_type = mime_type


class UploadPartResponse():
  """
  S3 response object for `UploadPart` command
  """
  def __init__(self, etag, part_number):
    self.etag = etag
    self.part_number = part_number


class CompleteMultipartUploadRequest():
  """
  S3 request object for `UploadPart` command
  """
  def __init__(self, bucket, object, upload_id, parts):
    # parts is a list of objects of the form
    # {'ETag': response.etag, 'PartNumber': response.part_number}
    self.bucket = bucket
    self.object = object
    self.upload_id = upload_id
    self.parts = parts
    # self.mime_type = mime_type


class ListRequest():
  """
  S3 request object for `List` command
  """
  def __init__(self, bucket, prefix, continuation_token=None):
    self.bucket = bucket
    self.prefix = prefix
    self.continuation_token = continuation_token


class ListResponse():
  """
  S3 response object for `List` command
  """
  def __init__(self, items, next_token=None):
    self.items = items
    self.next_token = next_token


class Item():
  """
  An item in S3
  """
  def __init__(self, etag, key, last_modified, size, mime_type=None):
    self.etag = etag
    self.key = key
    self.last_modified = last_modified
    self.size = size
    self.mime_type = mime_type


class DeleteRequest():
  """
  S3 request object for `Delete` command
  """
  def __init__(self, bucket, object):
    self.bucket = bucket
    self.object = object


class DeleteBatchRequest():
  def __init__(self, bucket, objects):
    # `objects` is a list of strings corresponding to the keys to be deleted
    # in the bucket
    self.bucket = bucket
    self.objects = objects


class DeleteBatchResponse():
  def __init__(self, deleted, failed, errors):
    # `deleted` is a list of strings corresponding to the keys that were deleted
    # `failed` is a list of strings corresponding to the keys that caused errors
    # `errors` is a list of S3ClientErrors, aligned with the order of `failed`
    self.deleted = deleted
    self.failed = failed
    self.errors = errors


class CopyRequest():
  def __init__(self, src_bucket, src_key, dest_bucket, dest_key):
    self.src_bucket = src_bucket
    self.src_key = src_key
    self.dest_bucket = dest_bucket
    self.dest_key = dest_key


class S3ClientError(Exception):
  def __init__(self, message=None, code=None):
    self.message = message
    self.code = code
