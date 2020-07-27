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

"""Metadata for use in BigQueryIO, i.e. a job_id to use in BQ job labels."""

# pytype: skip-file

from __future__ import absolute_import

import re

from apache_beam.io.gcp import gce_metadata_util

_VALID_CLOUD_LABEL_PATTERN = re.compile(r'^[a-z0-9\_\-]{1,63}$')


def _is_valid_cloud_label_value(label_value):
  """Returns true if label_value is a valid cloud label string.

    This function can return false in cases where the label value is valid.
    However, it will not return true in a case where the lavel value is invalid.
    This is because a stricter set of allowed characters is used in this
    validator, because foreign language characters are not accepted.
    Thus, this should not be used as a generic validator for all cloud labels.

    See Also:
      https://cloud.google.com/compute/docs/labeling-resources

    Args:
      label_value: The label value to validate.

    Returns:
      True if the label value is a valid
  """
  return _VALID_CLOUD_LABEL_PATTERN.match(label_value)


def create_bigquery_io_metadata():
  """Creates a BigQueryIOMetadata.

  This will request metadata properly based on which runner is being used.
  """
  dataflow_job_id = gce_metadata_util.fetch_dataflow_job_id()
  # If a dataflow_job id is returned on GCE metadata. Then it means
  # This program is running on a Dataflow GCE VM.
  is_dataflow_runner = bool(dataflow_job_id)
  kwargs = {}
  if is_dataflow_runner:
    # Only use this label if it is validated already.
    # As we do not want a bad label to fail the BQ job.
    if _is_valid_cloud_label_value(dataflow_job_id):
      kwargs['beam_job_id'] = dataflow_job_id
  return BigQueryIOMetadata(**kwargs)


class BigQueryIOMetadata(object):
  """Metadata class for BigQueryIO. i.e. to use as BQ job labels.

  Do not construct directly, use the create_bigquery_io_metadata factory.
  Which will request metadata properly based on which runner is being used.
  """
  def __init__(self, beam_job_id=None):
    self.beam_job_id = beam_job_id

  def add_additional_bq_job_labels(self, job_labels=None):
    job_labels = job_labels or {}
    if self.beam_job_id and 'beam_job_id' not in job_labels:
      job_labels['beam_job_id'] = self.beam_job_id
    return job_labels
