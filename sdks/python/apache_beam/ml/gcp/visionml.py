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
  from google.cloud import vision
except ImportError:
  raise ImportError(
      'Google Cloud Vision not supported for this execution environment '
      '(could not import google.cloud.vision).')

__all__ = [
    'AnnotateImage',
    'AnnotateImageWithContext',
    'AsyncBatchAnnotateImage',
    'AsyncBatchAnnotateImageWithContext'
]


@ttl_cache(maxsize=128, ttl=3600)
def get_vision_client(client_options=None):
  """Returns a Cloud Vision API client."""
  _client = vision.ImageAnnotatorClient(client_options=client_options)
  return _client


class AnnotateImage(PTransform):
  """A ``PTransform`` for annotating images using the GCP Vision API
  ref: https://cloud.google.com/vision/docs/

  Sends each element to the GCP Vision API. Element is a
  Union[text_type, binary_type] of either an URI (e.g. a GCS URI) or binary_type
  base64-encoded image data.
  Accepts an `AsDict` side input that maps each image to an image context.
  """
  def __init__(
      self,
      features,
      retry=None,
      timeout=120,
      client_options=None,
      context_side_input=None):
    """
    Args:
      features: (List[``vision.types.Feature.enums.Feature``]) Required.
        The Vision API features to detect
      retry: (google.api_core.retry.Retry) Optional.
        A retry object used to retry requests.
        If None is specified (default), requests will not be retried.
      timeout: (float) Optional.
        The time in seconds to wait for the response from the
        Vision API.
      client_options: (Union[dict, google.api_core.client_options.ClientOptions])
        Client options used to set user options on the client.
        API Endpoint should be set through client_options.
      context_side_input: (beam.pvalue.AsDict) Optional.
        An ``AsDict`` of a PCollection to be passed to the
        _ImageAnnotateFn as the image context mapping containing additional
        image context and/or feature-specific parameters.
        Example usage::

          image_contexts =
            [(''gs://cloud-samples-data/vision/ocr/sign.jpg'', Union[dict,
            ``vision.types.ImageContext()``]),
            (''gs://cloud-samples-data/vision/ocr/sign.jpg'', Union[dict,
            ``vision.types.ImageContext()``]),]

          context_side_input =
            (
              p
              | "Image contexts" >> beam.Create(image_contexts)
            )

          visionml.AnnotateImage(features,
            context_side_input=beam.pvalue.AsDict(context_side_input)))
    """
    super(AnnotateImage, self).__init__()
    self.features = features
    self.retry = retry
    self.timeout = timeout
    self.client_options = client_options
    self.context_side_input = context_side_input

  def expand(self, pvalue):
    return pvalue | ParDo(
        _ImageAnnotateFn(
            features=self.features,
            retry=self.retry,
            timeout=self.timeout,
            client_options=self.client_options),
        context_side_input=self.context_side_input)


@typehints.with_input_types(
    Union[text_type, binary_type], Optional[vision.types.ImageContext])
class _ImageAnnotateFn(DoFn):
  """A DoFn that sends each input element to the GCP Vision API
  service and outputs an element with the return result of the API
  (``google.cloud.vision_v1.types.AnnotateImageResponse``).
  """
  def __init__(self, features, retry, timeout, client_options):
    super(_ImageAnnotateFn, self).__init__()
    self._client = None
    self.features = features
    self.retry = retry
    self.timeout = timeout
    self.client_options = client_options
    self.counter = Metrics.counter(self.__class__, "API Calls")

  def start_bundle(self):
    self._client = get_vision_client(self.client_options)

  def _annotate_image(self, element, image_context):
    if isinstance(element, text_type):
      image = vision.types.Image(
          source=vision.types.ImageSource(image_uri=element))
    else:  # Typehint checks only allows text_type or binary_type
      image = vision.types.Image(content=element)

    request = vision.types.AnnotateImageRequest(
        image=image, features=self.features, image_context=image_context)
    response = self._client.annotate_image(
        request=request, retry=self.retry, timeout=self.timeout)

    return response

  def process(self, element, context_side_input=None, *args, **kwargs):
    if context_side_input:  # If we have a side input image context, use that
      image_context = context_side_input.get(element)
    else:
      image_context = None
    response = self._annotate_image(element, image_context)
    self.counter.inc()
    yield response


class AnnotateImageWithContext(AnnotateImage):
  """A ``PTransform`` for annotating images using the GCP Vision API
  ref: https://cloud.google.com/vision/docs/

  Sends each element to the GCP Vision API. Element is a tuple of

      (Union[text_type, binary_type],
      Optional[``vision.types.ImageContext``])

  where the former is either an URI (e.g. a GCS URI) or binary_type
  base64-encoded image data.
  """
  def __init__(self, features, retry=None, timeout=120, client_options=None):
    """
     Args:
      features: (List[``vision.types.Feature.enums.Feature``]) Required.
        The Vision API features to detect
      retry: (google.api_core.retry.Retry) Optional.
        A retry object used to retry requests.
        If None is specified (default), requests will not be retried.
      timeout: (float) Optional.
        The time in seconds to wait for the response from the
        Vision API
      client_options: (Union[dict, google.api_core.client_options.ClientOptions])
        Client options used to set user options on the client.
        API Endpoint should be set through client_options.
    """
    super(AnnotateImageWithContext, self).__init__(
        features=features,
        retry=retry,
        timeout=timeout,
        client_options=client_options)

  def expand(self, pvalue):
    return pvalue | ParDo(
        _ImageAnnotateFnWithContext(
            features=self.features,
            retry=self.retry,
            timeout=self.timeout,
            client_options=self.client_options))


@typehints.with_input_types(
    Tuple[Union[text_type, binary_type], Optional[vision.types.ImageContext]])
class _ImageAnnotateFnWithContext(_ImageAnnotateFn):
  """A DoFn that unpacks each input tuple to element, image_context variables
  and sends these to the GCP Vision API service and outputs an element with the
  return result of the API
  (``google.cloud.vision_v1.types.AnnotateImageResponse``).
  """
  def __init__(self, features, retry, timeout, client_options):
    super(_ImageAnnotateFnWithContext, self).__init__(
        features=features,
        retry=retry,
        timeout=timeout,
        client_options=client_options)

  def process(self, element, *args, **kwargs):
    element, image_context = element  # Unpack (image, image_context) tuple
    response = self._annotate_image(element, image_context)
    self.counter.inc()
    yield response


class AsyncBatchAnnotateImage(PTransform):
  """A ``PTransform`` for batch (offline) annotating images using the
  GCP Vision API. ref: https://cloud.google.com/vision/docs/

  Sends each batch of elements to the GCP Vision API which then stores the
  results in GCS.
  Element is a Union[text_type, binary_type] of either an URI (e.g. a GCS URI)
  or binary_type base64-encoded image data.
  Accepts an `AsDict` side input that maps each image to an image context.
  """

  MAX_BATCH_SIZE = 5000

  def __init__(
      self,
      features,
      output_config=None,
      gcs_destination=None,
      retry=None,
      timeout=120,
      batch_size=None,
      client_options=None,
      context_side_input=None,
      metadata=None):
    """
    Args:
      features: (List[``vision.types.Feature.enums.Feature``]) Required.
        The Vision API features to detect
      output_config:
        (Union[dict, ~google.cloud.vision.types.OutputConfig]) Optional.
        The desired output location and metadata (e.g. format).
        If a dict is provided, it must be of the same form as the protobuf
        message :class:`~google.cloud.vision.types.OutputConfig`
      gcs_destination: (str) Optional. The desired output location.
        Either output_config or gcs_destination needs to be set.
        output_config takes precedence.
      retry: (google.api_core.retry.Retry) Optional.
        A retry object used to retry requests.
        If None is specified (default), requests will not be retried.
      timeout: (float) Optional.
        The time in seconds to wait for the response from the Vision API.
        Default is 120 for single-element requests and 300 for batch annotation.
      batch_size: (int) Number of images to batch in the same request to the
        Vision API. Default is 5000.
      client_options: (Union[dict, google.api_core.client_options.ClientOptions])
        Client options used to set user options on the client.
        API Endpoint should be set through client_options.
      context_side_input: (beam.pvalue.AsDict) Optional.
        An ``AsDict`` of a PCollection to be passed to the
        _ImageAnnotateFn as the image context mapping containing additional
        image context and/or feature-specific parameters.
        Example usage::

          image_contexts =
            [(''gs://cloud-samples-data/vision/ocr/sign.jpg'', Union[dict,
            ``vision.types.ImageContext()``]),
            (''gs://cloud-samples-data/vision/ocr/sign.jpg'', Union[dict,
            ``vision.types.ImageContext()``]),]

          context_side_input =
            (
              p
              | "Image contexts" >> beam.Create(image_contexts)
            )

          visionml.AsyncBatchAnnotateImage(features, output_config,
            context_side_input=beam.pvalue.AsDict(context_side_input)))
      metadata: (Optional[Sequence[Tuple[str, str]]]): Optional.
        Additional metadata that is provided to the method.

    """
    super(AsyncBatchAnnotateImage, self).__init__()
    self.features = features
    self.output_config = output_config
    if output_config is None and gcs_destination is None:
      raise ValueError('output_config or gcs_destination must be specified')
    if output_config is None:
      self.output_config = self._generate_output_config(gcs_destination)

    self.retry = retry
    self.timeout = timeout
    self.batch_size = batch_size or AsyncBatchAnnotateImage.MAX_BATCH_SIZE
    if self.batch_size > AsyncBatchAnnotateImage.MAX_BATCH_SIZE:
      raise ValueError(
          'Max batch_size exceeded. '
          'Batch size needs to be smaller than {}'.format(
              AsyncBatchAnnotateImage.MAX_BATCH_SIZE))
    self.client_options = client_options
    self.context_side_input = context_side_input
    self.metadata = metadata

  @staticmethod
  def _generate_output_config(output_uri):
    gcs_destination = {"uri": output_uri}
    output_config = {"gcs_destination": gcs_destination}
    return output_config

  def expand(self, pvalue):
    return pvalue | ParDo(
        _AsyncBatchImageAnnotateFn(
            features=self.features,
            output_config=self.output_config,
            retry=self.retry,
            timeout=self.timeout,
            batch_size=self.batch_size,
            client_options=self.client_options,
            metadata=self.metadata),
        context_side_input=self.context_side_input)


class AsyncBatchAnnotateImageWithContext(AsyncBatchAnnotateImage):
  """A ``PTransform`` for batch (offline) annotating images using the
  GCP Vision API. ref: https://cloud.google.com/vision/docs/batch

  Sends each batch of elements to the GCP Vision API which then stores the
  results in GCS.
  Element is a Union[text_type, binary_type] of either an URI (e.g. a GCS URI)
  or binary_type base64-encoded image data.
  Accepts an `AsDict` side input that maps each image to an image context.
  """
  def __init__(
      self,
      features,
      output_config=None,
      gcs_destination=None,
      retry=None,
      timeout=120,
      batch_size=None,
      client_options=None,
      metadata=None):
    """
    Args:
      features: (List[``vision.types.Feature.enums.Feature``]) Required.
        The Vision API features to detect
      output_config:
        (Union[dict, ~google.cloud.vision.types.OutputConfig]) Optional.
        The desired output location and metadata (e.g. format).
        If a dict is provided, it must be of the same form as the protobuf
        message :class:`~google.cloud.vision.types.OutputConfig`
      gcs_destination: (str) Optional. The desired output location.
        Either output_config or gcs_destination needs to be set.
        output_config takes precedence.
      retry: (google.api_core.retry.Retry) Optional.
        A retry object used to retry requests.
        If None is specified (default), requests will not be retried.
      timeout: (float) Optional.
        The time in seconds to wait for the response from the Vision API.
        Default is 120 for single-element requests and 300 for batch annotation.
      batch_size: (int) Number of images to batch in the same request to the
        Vision API. Default is 5000.
      client_options: (Union[dict, google.api_core.client_options.ClientOptions])
        Client options used to set user options on the client.
        API Endpoint should be set through client_options.
      metadata: (Optional[Sequence[Tuple[str, str]]]): Optional.
        Additional metadata that is provided to the method.
    """
    super(AsyncBatchAnnotateImageWithContext, self).__init__(
        features=features,
        output_config=output_config,
        gcs_destination=gcs_destination,
        retry=retry,
        timeout=timeout,
        batch_size=batch_size,
        client_options=client_options,
        metadata=metadata)

  def expand(self, pvalue):
    return pvalue | ParDo(
        _AsyncBatchImageAnnotateFnWithContext(
            features=self.features,
            output_config=self.output_config,
            retry=self.retry,
            timeout=self.timeout,
            batch_size=self.batch_size,
            client_options=self.client_options,
            metadata=self.metadata))


@typehints.with_input_types(
    Union[text_type, binary_type], Optional[vision.types.ImageContext])
class _AsyncBatchImageAnnotateFn(DoFn):
  """A DoFn that sends each input element to the GCP Vision API
  service in batches and stores the results in GCS. Returns a
  ``google.cloud.vision.types.AsyncBatchAnnotateImagesResponse`` containing the
  the output location and metadata from AsyncBatchAnnotateImagesRequest.
  """
  def __init__(
      self,
      features,
      output_config,
      retry,
      timeout,
      client_options,
      batch_size,
      metadata):

    super(_AsyncBatchImageAnnotateFn, self).__init__()
    self._client = None
    self.features = features
    self.output_config = output_config
    self.retry = retry
    self.timeout = timeout
    self.client_options = client_options
    self.batch_size = batch_size
    self._batch_elements = None
    self.metadata = metadata
    self.counter = Metrics.counter(self.__class__, "API Calls")

  def start_bundle(self):
    self._client = get_vision_client(self.client_options)
    self._batch_elements = []

  def finish_bundle(self):
    if self._batch_elements:
      response = self._flush_batch()
      self.counter.inc()
      return response

  def create_annotate_image_request(self, element, image_context):
    if isinstance(element, text_type):
      image = vision.types.Image(
          source=vision.types.ImageSource(image_uri=element))
    else:  # Typehint checks only allows text_type or binary_type
      image = vision.types.Image(content=element)

    request = vision.types.AnnotateImageRequest(
        image=image, features=self.features, image_context=image_context)
    return request

  def process(self, element, context_side_input=None, *args, **kwargs):
    if context_side_input:  # If we have a side input image context, use that
      image_context = context_side_input.get(element)
    else:
      image_context = None

    # Evaluate batches
    request = self.create_annotate_image_request(element, image_context)
    self._batch_elements.append(request)
    if len(self._batch_elements) >= self.batch_size or \
            len(self._batch_elements) >= AsyncBatchAnnotateImage.MAX_BATCH_SIZE:
      response = self._flush_batch()
      self.counter.inc()
      yield response

  def _flush_batch(self):
    operation = self._client.async_batch_annotate_images(
        requests=self._batch_elements,
        output_config=self.output_config,
        retry=self.retry,
        timeout=self.timeout,
        metadata=self.metadata)
    self._batch_elements = []
    return operation


@typehints.with_input_types(
    Tuple[Union[text_type, binary_type], Optional[vision.types.ImageContext]])
class _AsyncBatchImageAnnotateFnWithContext(_AsyncBatchImageAnnotateFn):
  """A DoFn that sends each input element and its context to the GCP Vision API
  service in batches. Returns a
  ``google.cloud.vision.types.AsyncBatchAnnotateImagesResponse`` containing the
  the output location and metadata from AsyncBatchAnnotateImagesRequest.
  """
  def __init__(
      self,
      features,
      output_config,
      retry,
      timeout,
      batch_size,
      client_options,
      metadata):
    super(_AsyncBatchImageAnnotateFnWithContext, self).__init__(
        features=features,
        output_config=output_config,
        retry=retry,
        timeout=timeout,
        client_options=client_options,
        batch_size=batch_size,
        metadata=metadata)

  def process(self, element, context_side_input=None, *args, **kwargs):
    element, image_context = element  # Unpack (image, image_context) tuple

    # Evaluate batches
    request = self.create_annotate_image_request(element, image_context)
    self._batch_elements.append(request)
    if len(self._batch_elements) >= self.batch_size or \
            len(self._batch_elements) >= self.MAX_BATCH_SIZE:
      response = self._flush_batch()
      self.counter.inc()
      yield response
