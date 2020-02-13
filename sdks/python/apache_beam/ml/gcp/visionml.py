# pylint: skip-file
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
A connector for sending API requests to the GCP Vision API.
"""

# TODO: Add Batching support https://google-cloud-python.readthedocs.io/en/0.32.0/vision/gapic/v1/api.html#google.cloud.vision_v1.ImageAnnotatorClient.batch_annotate_images

from __future__ import absolute_import, print_function

from cachetools.func import ttl_cache
from future.utils import binary_type, text_type
from typing import Tuple
from typing import Union

from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.transforms import DoFn, ParDo, PTransform

try:
  from google.cloud import vision
except ImportError:
  raise ImportError(
      'Google Cloud Vision not supported for this execution environment '
      '(could not import google.cloud.vision).')

__all__ = ['AnnotateImage']


@ttl_cache(maxsize=128, ttl=3600)
def get_vision_client(client_options=None):
  """Returns a Cloud Vision API client."""
  _client = vision.ImageAnnotatorClient(client_options=client_options)
  return _client


class AnnotateImage(PTransform):
  """A ``PTransform`` for annotating images using the GCP Vision API
  ref: https://cloud.google.com/vision/docs/
  """
  def __init__(self, features, retry=None, timeout=120, client_options=None):
    super(AnnotateImage, self).__init__()
    self.features = features
    self.retry = retry
    self.timeout = timeout
    self.client_options = client_options

  def expand(self, pvalue):
    return pvalue | ParDo(
        self._ImageAnnotateFn(
            features=self.features,
            retry=self.retry,
            timeout=self.timeout,
            client_options=self.client_options))

  @typehints.with_input_types(
      Union[Tuple[Union[text_type, binary_type], vision.types.ImageContext],
            Union[text_type, binary_type]])
  class _ImageAnnotateFn(DoFn):
    """ A DoFn that sends each input element to the GCP Vision API
        service and outputs an element with the return result of the API
        (``google.cloud.vision_v1.types.BatchAnnotateImagesResponse``).
     """
    def __init__(self, features, retry, timeout, client_options):
      super(AnnotateImage._ImageAnnotateFn, self).__init__()
      self._client = None
      self.features = features
      self.retry = retry
      self.timeout = timeout
      self.client_options = client_options
      self.image_context = None
      self.counter = Metrics.counter(self.__class__, "API Calls")

    def start_bundle(self):
      self._client = get_vision_client(self.client_options)

    def process(self, element, *args, **kwargs):
      if isinstance(element, Tuple):  # Unpack (element, ImageContext) tuple
        element, self.image_context = element
      if isinstance(element, text_type):  # Is element an URI
        image = vision.types.Image(
            source=vision.types.ImageSource(image_uri=element))
      elif isinstance(element, binary_type):  # Is element raw bytes
        image = vision.types.Image(content=element)

      request = vision.types.AnnotateImageRequest(
          image=image, features=self.features, image_context=self.image_context)
      response = self._client.annotate_image(
          request=request, retry=self.retry, timeout=self.timeout)

      self.counter.inc()
      yield response
