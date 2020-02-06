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

from future.utils import binary_type, text_type
from typing import Union

from apache_beam import typehints
from apache_beam.ml.gcp import video_intelligence_helper as helper
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn, ParDo, PTransform

__all__ = ['AnnotateVideo']


class AnnotateVideo(PTransform):
  """A ``PTransform`` for annotating video using the GCP Video Intelligence API
  ref: https://cloud.google.com/video-intelligence/docs
  """
  def __init__(
      self, features, video_context=None, location_id=None, metadata=None):
    """
      Args:
        features: (List[``videointelligence_v1.enums.Feature``]) Required.
          the Video Intelligence API features to detect
        video_context: (dict, ``videointelligence_v1.types.VideoContext``)
          Optional.
          Additional video context and/or feature-specific parameters.
        location_id: (str) Optional.
          Cloud region where annotation should take place.
          If no region is specified, a region will be determined
          based on video file location.
        metadata: (Sequence[Tuple[str, str]]) Optional.
          Additional metadata that is provided to the method.
    """
    super(AnnotateVideo, self).__init__()
    self.features = features
    self.video_context = video_context
    self.location_id = location_id
    self.metadata = metadata

  def expand(self, pvalue):
    return pvalue | ParDo(
        self._VideoAnnotateFn(
            features=self.features,
            video_context=self.video_context,
            location_id=self.location_id,
            metadata=self.metadata))

  @typehints.with_input_types(Union[text_type, binary_type])
  class _VideoAnnotateFn(DoFn):
    """ A ``DoFn`` that sends every element to the GCP Video Intelligence API
      and returns a PCollection of
    ``google.cloud.videointelligence_v1.types.AnnotateVideoResponse``.
     """
    def __init__(self, features, video_context, location_id, metadata):
      super(AnnotateVideo._VideoAnnotateFn, self).__init__()
      self._client = None
      self.features = features
      self.video_context = video_context
      self.location_id = location_id
      self.metadata = metadata
      self.counter = Metrics.counter(self.__class__, "API Calls")

    def start_bundle(self):
      self._client = helper.get_videointelligence_client()

    def process(self, element, *args, **kwargs):
      if isinstance(element, text_type):  # Is element an URI to a GCS bucket
        response = self._client.annotate_video(
            input_uri=element,
            features=self.features,
            video_context=self.video_context,
            location_id=self.location_id,
            metadata=self.metadata)
      elif isinstance(element, binary_type):  # Is element raw bytes
        response = self._client.annotate_video(
            input_content=element,
            features=self.features,
            video_context=self.video_context,
            location_id=self.location_id,
            metadata=self.metadata)
      else:
        raise TypeError(
            "{}: input element needs to be either {} or {}"
            " got {} instead".format(
                self.__class__.__name__, text_type, binary_type, type(element)))
      self.counter.inc()
      yield response.result(timeout=120)
