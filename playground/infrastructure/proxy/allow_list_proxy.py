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
from mitmproxy import http

from allow_list import ALLOWED_LIST, ALLOWED_BUCKET_LIST

GCS_HOST = "storage.googleapis.com"


def request(flow: http.HTTPFlow) -> None:
  allowed_bucket = flow.request.pretty_host == GCS_HOST and \
  (flow.request.path.split("/")[1] in ALLOWED_BUCKET_LIST or \
  flow.request.path.split("/")[4] in ALLOWED_BUCKET_LIST)
  allowed_host = flow.request.pretty_host in ALLOWED_LIST
  if not (allowed_bucket or allowed_host):
    flow.response = http.Response.make(
        status_code=403,
        content="Making requests to the hosts that are not listed "
        "in the allowed list is forbidden. "
        "host:" + flow.request.pretty_host + ", path: " + flow.request.path)
