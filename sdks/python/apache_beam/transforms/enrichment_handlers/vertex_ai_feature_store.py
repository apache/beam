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
import logging

__all__ = []

from apache_beam.io.requestresponse import RequestT, ResponseT
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

_LOGGER = logging.getLogger(__name__)


class VertexAIFeatureStoreEnrichmentHandler(EnrichmentSourceHandler):
  def __init__(self):
    pass

  def __enter__(self):
    pass

  def __call__(self, request: RequestT, *args, **kwargs) -> ResponseT:
    pass

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass

  def get_cache_request_key(self):
    pass
