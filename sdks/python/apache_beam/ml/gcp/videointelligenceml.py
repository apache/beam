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

"""A connector for sending API requests to the GCP Video Intelligence API."""

from __future__ import absolute_import

from typing import Optional
from typing import Tuple
from typing import Union

from future.utils import binary_type
from future.utils import text_type

from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from cachetools.func import ttl_cache

try:
  from google.cloud import videointelligence
except ImportError:
  raise ImportError(
      'Google Cloud Video Intelligence not supported for this execution '
      'environment (could not import google.cloud.videointelligence).')

__all__ = ['AnnotateVideo', 'AnnotateVideoWithContext']


@ttl_cache(maxsize=128, ttl=3600)
def get_videointelligence_client():
  """Returns a Cloud Video Intelligence client."""
  _client = videointelligence.VideoIntelligenceServiceClient()
  return _client


class AnnotateVideo(PTransform):
  """A ``PTransform`` for annotating video using the GCP Video Intelligence API
  ref: https://cloud.google.com/video-intelligence/docs

  Sends each element to the GCP Video Intelligence API. Element is a
  Union[text_type, binary_type] of either an URI (e.g. a GCS URI) or
  binary_type base64-encoded video data.
  Accepts an `AsDict` side input that maps each video to a video context.
  """
  def __init__(
      self,
      features,
      location_id=None,
      metadata=None,
      timeout=120,
      context_side_input=None):
    """
    Args:
      features: (List[``videointelligence_v1.enums.Feature``]) Required.
        The Video Intelligence API features to detect
      location_id: (str) Optional.
        Cloud region where annotation should take place.
        If no region is specified, a region will be determined
        based on video file location.
      metadata: (Sequence[Tuple[str, str]]) Optional.
        Additional metadata that is provided to the method.
      timeout: (int) Optional.
        The time in seconds to wait for the response from the
        Video Intelligence API
      context_side_input: (beam.pvalue.AsDict) Optional.
        An ``AsDict`` of a PCollection to be passed to the
        _VideoAnnotateFn as the video context mapping containing additional
        video context and/or feature-specific parameters.
        Example usage::

          video_contexts =
            [('gs://cloud-samples-data/video/cat.mp4', Union[dict,
            ``videointelligence_v1.types.VideoContext``]),
            ('gs://some-other-video/sample.mp4', Union[dict,
            ``videointelligence_v1.types.VideoContext``]),]

          context_side_input =
            (
              p
              | "Video contexts" >> beam.Create(video_contexts)
            )

          videointelligenceml.AnnotateVideo(features,
            context_side_input=beam.pvalue.AsDict(context_side_input)))
    """
    super(AnnotateVideo, self).__init__()
    self.features = features
    self.location_id = location_id
    self.metadata = metadata
    self.timeout = timeout
    self.context_side_input = context_side_input

  def expand(self, pvalue):
    return pvalue | ParDo(
        _VideoAnnotateFn(
            features=self.features,
            location_id=self.location_id,
            metadata=self.metadata,
            timeout=self.timeout),
        context_side_input=self.context_side_input)


@typehints.with_input_types(
    Union[text_type, binary_type],
    Optional[videointelligence.types.VideoContext])
class _VideoAnnotateFn(DoFn):
  """A DoFn that sends each input element to the GCP Video Intelligence API
  service and outputs an element with the return result of the API
  (``google.cloud.videointelligence_v1.types.AnnotateVideoResponse``).
  """
  def __init__(self, features, location_id, metadata, timeout):
    super(_VideoAnnotateFn, self).__init__()
    self._client = None
    self.features = features
    self.location_id = location_id
    self.metadata = metadata
    self.timeout = timeout
    self.counter = Metrics.counter(self.__class__, "API Calls")

  def start_bundle(self):
    self._client = get_videointelligence_client()

  def _annotate_video(self, element, video_context):
    if isinstance(element, text_type):  # Is element an URI to a GCS bucket
      response = self._client.annotate_video(
          input_uri=element,
          features=self.features,
          video_context=video_context,
          location_id=self.location_id,
          metadata=self.metadata)
    else:  # Is element raw bytes
      response = self._client.annotate_video(
          input_content=element,
          features=self.features,
          video_context=video_context,
          location_id=self.location_id,
          metadata=self.metadata)
    return response

  def process(self, element, context_side_input=None, *args, **kwargs):
    if context_side_input:  # If we have a side input video context, use that
      video_context = context_side_input.get(element)
    else:
      video_context = None
    response = self._annotate_video(element, video_context)
    self.counter.inc()
    yield response.result(timeout=self.timeout)


class AnnotateVideoWithContext(AnnotateVideo):
  """A ``PTransform`` for annotating video using the GCP Video Intelligence API
  ref: https://cloud.google.com/video-intelligence/docs

  Sends each element to the GCP Video Intelligence API.
  Element is a tuple of

    (Union[text_type, binary_type],
    Optional[videointelligence.types.VideoContext])

  where the former is either an URI (e.g. a GCS URI) or
  binary_type base64-encoded video data
  """
  def __init__(self, features, location_id=None, metadata=None, timeout=120):
    """
      Args:
        features: (List[``videointelligence_v1.enums.Feature``]) Required.
          the Video Intelligence API features to detect
        location_id: (str) Optional.
          Cloud region where annotation should take place.
          If no region is specified, a region will be determined
          based on video file location.
        metadata: (Sequence[Tuple[str, str]]) Optional.
          Additional metadata that is provided to the method.
        timeout: (int) Optional.
          The time in seconds to wait for the response from the
          Video Intelligence API
    """
    super(AnnotateVideoWithContext, self).__init__(
        features=features,
        location_id=location_id,
        metadata=metadata,
        timeout=timeout)

  def expand(self, pvalue):
    return pvalue | ParDo(
        _VideoAnnotateFnWithContext(
            features=self.features,
            location_id=self.location_id,
            metadata=self.metadata,
            timeout=self.timeout))


@typehints.with_input_types(
    Tuple[Union[text_type, binary_type],
          Optional[videointelligence.types.VideoContext]])
class _VideoAnnotateFnWithContext(_VideoAnnotateFn):
  """A DoFn that unpacks each input tuple to element, video_context variables
  and sends these to the GCP Video Intelligence API service and outputs
  an element with the return result of the API
  (``google.cloud.videointelligence_v1.types.AnnotateVideoResponse``).
  """
  def __init__(self, features, location_id, metadata, timeout):
    super(_VideoAnnotateFnWithContext, self).__init__(
        features=features,
        location_id=location_id,
        metadata=metadata,
        timeout=timeout)

  def process(self, element, *args, **kwargs):
    element, video_context = element  # Unpack (video, video_context) tuple
    response = self._annotate_video(element, video_context)
    self.counter.inc()
    yield response.result(timeout=self.timeout)
