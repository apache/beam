#  /*
#   * Licensed to the Apache Software Foundation (ASF) under one
#   * or more contributor license agreements.  See the NOTICE file
#   * distributed with this work for additional information
#   * regarding copyright ownership.  The ASF licenses this file
#   * to you under the Apache License, Version 2.0 (the
#   * "License"); you may not use this file except in compliance
#   * with the License.  You may obtain a copy of the License at
#   *
#   *     http://www.apache.org/licenses/LICENSE-2.0
#   *
#   * Unless required by applicable law or agreed to in writing, software
#   * distributed under the License is distributed on an "AS IS" BASIS,
#   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   * See the License for the specific language governing permissions and
#   * limitations under the License.
#   */

"""``PTransforms`` that implement Google Cloud Data Loss Prevention
    functionality.
"""

from __future__ import absolute_import

import logging

from google.cloud import dlp_v2

import apache_beam as beam
from apache_beam.utils import retry
from apache_beam.utils.annotations import experimental

__all__ = ['MaskDetectedDetails', 'InspectForDetails']

_LOGGER = logging.getLogger(__name__)


@experimental()
class MaskDetectedDetails(beam.PTransform):
  """Scrubs sensitive information detected in text.
  The ``PTransform`` returns a ``PCollection`` of ``str``
  Example usage::
    pipeline | MaskDetectedDetails(project='example-gcp-project',
      deidentification_config={
          'info_type_transformations: {
              'transformations': [{
                  'primitive_transformation': {
                      'character_mask_config': {
                          'masking_character': '#'
                      }
                  }
              }]
          }
      }, inspection_config={'info_types': [{'name': 'EMAIL_ADDRESS'}]})
  """
  def __init__(
      self,
      project=None,
      deidentification_template_name=None,
      deidentification_config=None,
      inspection_template_name=None,
      inspection_config=None,
      timeout=None):
    """Initializes a :class:`MaskDetectedDetails` transform.
    Args:
      project (str): Required. GCP project in which the data processing is
        to be done
      deidentification_template_name (str): Either this or
        `deidentification_config` required. Name of
        deidentification template to be used on detected sensitive information
        instances in text.
      deidentification_config
        (``Union[dict, google.cloud.dlp_v2.types.DeidentifyConfig]``):
        Configuration for the de-identification of the content item.
      inspection_template_name (str): This or `inspection_config` required.
        Name of inspection template to be used
        to detect sensitive data in text.
      inspection_config
        (``Union[dict, google.cloud.dlp_v2.types.InspectConfig]``):
        Configuration for the inspector used to detect sensitive data in text.
      timeout (float): Optional. The amount of time, in seconds, to wait for
        the request to complete.
    """
    self.config = {}
    self.project = project
    self.timeout = timeout
    if project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" property')
    if deidentification_template_name is not None \
        and deidentification_config is not None:
      raise ValueError(
          'Both deidentification_template_name and '
          'deidentification_config were specified.'
          ' Please specify only one of these.')
    elif deidentification_template_name is None \
        and deidentification_config is None:
      raise ValueError(
          'deidentification_template_name or '
          'deidentification_config must be specified.')
    elif deidentification_template_name is not None:
      self.config['deidentify_template_name'] = deidentification_template_name
    else:
      self.config['deidentify_config'] = deidentification_config

    if inspection_template_name is not None and inspection_config is not None:
      raise ValueError(
          'Both inspection_template_name and '
          'inspection_template were specified.'
          ' Please specify ony one of these.')
    elif inspection_config is None and inspection_template_name is None:
      raise ValueError(
          'inspection_template_name or inspection_config must be specified')
    elif inspection_template_name is not None:
      self.config['inspect_template_name'] = inspection_template_name
    elif inspection_config is not None:
      self.config['inspect_config'] = inspection_config

  def expand(self, pcoll):
    return (
        pcoll
        | beam.ParDo(_DeidentifyFn(self.config, self.timeout, self.project)))


@experimental()
class InspectForDetails(beam.PTransform):
  """Inspects input text for sensitive information.
  the ``PTransform`` returns a ``PCollection`` of
  ``List[google.cloud.dlp_v2.proto.dlp_pb2.Finding]``
  Example usage::
      pipeline | InspectForDetails(project='example-gcp-project',
                inspection_config={'info_types': [{'name': 'EMAIL_ADDRESS'}]})
  """
  def __init__(
      self,
      inspection_template_name=None,
      inspection_config=None,
      project=None,
      timeout=None):
    """Initializes a :class:`InspectForDetails` transform.
    Args:
      inspection_template_name (str): This or `inspection_config` required.
        Name of inspection template to be used
        to detect sensitive data in text.
      inspection_config
        (``Union[dict, google.cloud.dlp_v2.types.InspectConfig]``):
        Configuration for the inspector used to detect sensitive data in text.
      project (str): Required. Name of GCP project in which the processing
        will take place.
      timeout (float): Optional. The amount of time, in seconds, to wait for
        the request to complete.
    """
    self.project = project
    self.timeout = timeout
    self.config = {}
    if project is None:
      raise ValueError(
          'GCP project name needs to be specified in "project" property')
    if inspection_template_name is not None and inspection_config is not None:
      raise ValueError(
          'Both inspection_template_name and '
          'inspection_template were specified.'
          ' Please specify ony one of these.')
    elif inspection_config is None and inspection_template_name is None:
      raise ValueError(
          'inspection_template_name or inspection_config must be specified')
    elif inspection_template_name is not None:
      self.config['inspect_template_name'] = inspection_template_name
    elif inspection_config is not None:
      self.config['inspect_config'] = inspection_config

  def expand(self, pcoll):
    return pcoll | beam.ParDo(
        _InspectFn(self.config, self.timeout, self.project))


class _DeidentifyFn(beam.DoFn):
  def __init__(self, config=None, timeout=None, project=None, client=None):
    self.config = config
    self.timeout = timeout
    self.client = client
    self.project = project

  def start_bundle(self):
    if self.client is None:
      self.client = dlp_v2.DlpServiceClient()

  def process(self, element, **kwargs):
    params = {
        'timeout': self.timeout,
        'retry': retry.with_exponential_backoff(
            retry_filter=retry.
            retry_on_server_errors_timeout_or_quota_issues_filter),
        'parent': self.client.project_path(self.project)
    }
    params.update(self.config)
    operation = self.client.deidentify_content(
        item={"value": element}, **params)

    yield operation.item.value


class _InspectFn(beam.DoFn):
  def __init__(self, config=None, timeout=None, project=None):
    self.config = config
    self.timeout = timeout
    self.client = None
    self.project = project

  def setup(self):
    self.client = dlp_v2.DlpServiceClient()

  def process(self, element, **kwargs):
    kwargs = {
        'timeout': self.timeout,
        'retry': retry.with_exponential_backoff(
            retry_filter=retry.
            retry_on_server_errors_timeout_or_quota_issues_filter),
        "parent": self.client.project_path(self.project)
    }
    kwargs.update(self.config)
    operation = self.client.inspect_content(item={"value": element}, **kwargs)
    hits = [x for x in operation.result.findings]
    yield hits
