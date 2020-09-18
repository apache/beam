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

from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.metrics import Metrics

try:
  from google.cloud import language
  from google.cloud.language import enums  # pylint: disable=unused-import
  from google.cloud.language import types
except ImportError:
  raise ImportError(
      'Google Cloud Natural Language API not supported for this execution '
      'environment (could not import Natural Language API client).')

__all__ = ['Document', 'AnnotateText']


class Document(object):
  """Represents the input to :class:`AnnotateText` transform.

  Args:
    content (str): The content of the input or the Google Cloud Storage URI
      where the file is stored.
    type (`Union[str, google.cloud.language.enums.Document.Type]`): Text type.
      Possible values are `HTML`, `PLAIN_TEXT`. The default value is
      `PLAIN_TEXT`.
    language_hint (`Optional[str]`): The language of the text. If not specified,
      language will be automatically detected. Values should conform to
      ISO-639-1 standard.
    encoding (`Optional[str]`): Text encoding. Possible values are: `NONE`,
     `UTF8`, `UTF16`, `UTF32`. The default value is `UTF8`.
    from_gcs (bool): Whether the content should be interpret as a Google Cloud
      Storage URI. The default value is :data:`False`.
  """

  def __init__(
      self,
      content: str,
      type: Union[str, enums.Document.Type] = 'PLAIN_TEXT',
      language_hint: Optional[str] = None,
      encoding: Optional[str] = 'UTF8',
      from_gcs: bool = False
  ):
    self.content = content
    self.type = type
    self.encoding = encoding
    self.language_hint = language_hint
    self.from_gcs = from_gcs

  @staticmethod
  def to_dict(document: Document) -> Mapping[str, Optional[str]]:
    if document.from_gcs:
      dict_repr = {'gcs_content_uri': document.content}
    else:
      dict_repr = {'content': document.content}
    dict_repr.update({
        'type': document.type, 'language': document.language_hint
    })
    return dict_repr


@beam.ptransform_fn
def AnnotateText(
    pcoll: beam.pvalue.PCollection,
    features: Union[Mapping[str, bool], types.AnnotateTextRequest.Features],
    timeout: Optional[float] = None,
    metadata: Optional[Sequence[Tuple[str, str]]] = None
):
  """A :class:`~apache_beam.transforms.ptransform.PTransform`
  for annotating text using the Google Cloud Natural Language API:
  https://cloud.google.com/natural-language/docs.

  Args:
    pcoll (:class:`~apache_beam.pvalue.PCollection`): An input PCollection of
      :class:`Document` objects.
    features (`Union[Mapping[str, bool], types.AnnotateTextRequest.Features]`):
      A dictionary of natural language operations to be performed on given
      text in the following format::

      {'extact_syntax'=True, 'extract_entities'=True}

    timeout (`Optional[float]`): The amount of time, in seconds, to wait
      for the request to complete. The timeout applies to each individual
      retry attempt.
    metadata (`Optional[Sequence[Tuple[str, str]]]`): Additional metadata
      that is provided to the method.
  """
  return pcoll | beam.ParDo(_AnnotateTextFn(features, timeout, metadata))


@beam.typehints.with_input_types(Document)
@beam.typehints.with_output_types(types.AnnotateTextResponse)
class _AnnotateTextFn(beam.DoFn):
  def __init__(
      self,
      features: Union[Mapping[str, bool], types.AnnotateTextRequest.Features],
      timeout: Optional[float],
      metadata: Optional[Sequence[Tuple[str, str]]] = None
  ):
    self.features = features
    self.timeout = timeout
    self.metadata = metadata
    self.api_calls = Metrics.counter(self.__class__.__name__, 'api_calls')
    self.client = None

  def setup(self):
    self.client = self._get_api_client()

  @staticmethod
  def _get_api_client() -> language.LanguageServiceClient:
    return language.LanguageServiceClient()

  def process(self, element):
    response = self.client.annotate_text(
        document=Document.to_dict(element),
        features=self.features,
        encoding_type=element.encoding,
        timeout=self.timeout,
        metadata=self.metadata)
    self.api_calls.inc()
    yield response
