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

"""Fetches GCE metadata if the calling process is running on a GCE VM."""

# pytype: skip-file

from __future__ import absolute_import

import requests

BASE_METADATA_URL = "http://metadata/computeMetadata/v1/"


def _fetch_metadata(key):
  try:
    headers = {"Metadata-Flavor": "Google"}
    uri = BASE_METADATA_URL + key
    resp = requests.get(uri, headers=headers, timeout=5)  # 5 seconds.
    if resp.status_code == 200:
      return resp.text
  except requests.exceptions.RequestException:
    # Silently fail, may mean its running on a non DataflowRunner,
    # in which case it's prefectly normal.
    pass
  return ""


def _fetch_custom_gce_metadata(customMetadataKey):
  return _fetch_metadata("instance/attributes/" + customMetadataKey)


def fetch_dataflow_job_id():
  return _fetch_custom_gce_metadata("job_id")
