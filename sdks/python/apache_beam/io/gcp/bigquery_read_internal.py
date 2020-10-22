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
Internal library for reading data from BigQuery.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""
import logging
import uuid
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms import PTransform

_LOGGER = logging.getLogger(__name__)


def bigquery_export_destination_uri(
    gcs_location_vp: Optional[ValueProvider],
    temp_location: Optional[str],
    unique_id: str,
    directory_only: bool = False,
) -> str:
  """Returns the fully qualified Google Cloud Storage URI where the
  extracted table should be written.
  """
  file_pattern = 'bigquery-table-dump-*.json'

  gcs_location = None
  if gcs_location_vp is not None:
    gcs_location = gcs_location_vp.get()

  if gcs_location is not None:
    gcs_base = gcs_location
  elif temp_location is not None:
    gcs_base = temp_location
    _LOGGER.debug("gcs_location is empty, using temp_location instead")
  else:
    raise ValueError(
        'ReadFromBigQuery requires a GCS location to be provided. Neither '
        'gcs_location in the constructor nor the fallback option '
        '--temp_location is set.')

  if not unique_id:
    unique_id = uuid.uuid4().hex

  if directory_only:
    return FileSystems.join(gcs_base, unique_id)
  else:
    return FileSystems.join(gcs_base, unique_id, file_pattern)


class _PassThroughThenCleanup(PTransform):
  """A PTransform that invokes a DoFn after the input PCollection has been
    processed.

    DoFn should have arguments (element, side_input, cleanup_signal).

    Utilizes readiness of PCollection to trigger DoFn.
  """
  def __init__(self, side_input=None):
    self.side_input = side_input

  def expand(self, input):
    class PassThrough(beam.DoFn):
      def process(self, element):
        yield element

    class RemoveExtractedFiles(beam.DoFn):
      def process(self, unused_element, unused_signal, gcs_locations):
        FileSystems.delete(list(gcs_locations))

    main_output, cleanup_signal = input | beam.ParDo(
        PassThrough()).with_outputs(
        'cleanup_signal', main='main')

    cleanup_input = input.pipeline | beam.Create([None])

    _ = cleanup_input | beam.ParDo(
        RemoveExtractedFiles(),
        beam.pvalue.AsSingleton(cleanup_signal),
        self.side_input,
    )

    return main_output
