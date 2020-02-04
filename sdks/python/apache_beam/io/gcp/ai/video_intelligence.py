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
"""
A connector for sending API requests to the GCP Video Intelligence API.
"""
from __future__ import absolute_import

from typing import Union

from apache_beam import typehints
from apache_beam.io.gcp.ai import helper
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn, ParDo, PTransform


class AnnotateVideo(PTransform):
  """A ``PTransform`` for annotating video using the GCP Video Intelligence API
  ref: https://cloud.google.com/video-intelligence/docs
  """

  def __init__(self, features):
    """
      Args:
        features: (List[``videointelligence.enums.Feature``])
          the Video Intelligence API features to detect
    """
    super(AnnotateVideo).__init__()
    self.features = features

  def expand(self, pvalue):
    return pvalue | ParDo(self._VideoAnnotateFn(features=self.features))

  @typehints.with_input_types(Union[str, bytes])
  class _VideoAnnotateFn(DoFn):
    """ A ``DoFn`` that sends every element to the GCP Video Intelligence API
      and returns a PCollection of
    ``google.cloud.videointelligence_v1.types.AnnotateVideoResponse``.
     """

    def __init__(self, features):
      super(AnnotateVideo._VideoAnnotateFn, self).__init__()
      self._client = None
      self.features = features
      self.counter = Metrics.counter(self.__class__, "API Calls")

    def start_bundle(self):
      self._client = helper.get_videointelligence_client()

    def process(self, element, *args, **kwargs):
      if isinstance(element, str):  # Is element an URI to a GCS bucket
        response = self._client.annotate_video(input_uri=element,
                                               features=self.features)
      elif isinstance(element, bytes):  # Is element raw bytes
        response = self._client.annotate_video(input_content=element,
                                               features=self.features)
      else:
        raise TypeError(
            "{}: input element needs to be either str or b64-encoded bytes"
            " got {} instead".format(self.__class__.__name__, type(element)))
      self.counter.inc()
      yield response.result(timeout=120)
