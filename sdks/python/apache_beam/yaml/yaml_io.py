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

"""This module contains the Python implementations for the builtin IOs.

They are referenced from standard_io.py.

Note that in the case that they overlap with other (likely Java)
implementations of the same transforms, the configs must be kept in sync.
"""

import os

import yaml

import apache_beam as beam
import apache_beam.io as beam_io
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.yaml import yaml_provider


def read_from_text(path: str):
  # TODO(yaml): Consider passing the filename and offset, possibly even
  # by default.
  return beam_io.ReadFromText(path) | beam.Map(lambda s: beam.Row(line=s))


@beam.ptransform_fn
def write_to_text(pcoll, path: str):
  try:
    field_names = [
        name for name, _ in named_fields_from_element_type(pcoll.element_type)
    ]
  except Exception as exn:
    raise ValueError(
        "WriteToText requires an input schema with exactly one field.") from exn
  if len(field_names) != 1:
    raise ValueError(
        "WriteToText requires an input schema with exactly one field, got %s" %
        field_names)
  sole_field_name, = field_names
  return pcoll | beam.Map(
      lambda x: str(getattr(x, sole_field_name))) | beam.io.WriteToText(path)


def read_from_bigquery(
    query=None, table=None, row_restriction=None, fields=None):
  if query is None:
    assert table is not None
  else:
    assert table is None and row_restriction is None and fields is None
  return ReadFromBigQuery(
      query=query,
      table=table,
      row_restriction=row_restriction,
      selected_fields=fields,
      method='DIRECT_READ',
      output_type='BEAM_ROW')


def write_to_bigquery(
    table,
    *,
    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=BigQueryDisposition.WRITE_APPEND,
    error_handling=None):
  class WriteToBigQueryHandlingErrors(beam.PTransform):
    def default_label(self):
      return 'WriteToBigQuery'

    def expand(self, pcoll):
      write_result = pcoll | WriteToBigQuery(
          table,
          method=WriteToBigQuery.Method.STORAGE_WRITE_API
          if error_handling else None,
          create_disposition=create_disposition,
          write_disposition=write_disposition,
          temp_file_format='AVRO')
      if error_handling and 'output' in error_handling:
        # TODO: Support error rates.
        return {
            'post_write': write_result.failed_rows_with_errors
            | beam.FlatMap(lambda x: None),
            error_handling['output']: write_result.failed_rows_with_errors
        }
      else:
        if write_result._method == WriteToBigQuery.Method.FILE_LOADS:
          # Never returns errors, just fails.
          return {
              'post_write': write_result.destination_load_jobid_pairs
              | beam.FlatMap(lambda x: None)
          }
        else:

          # This should likely be pushed into the BQ read itself to avoid
          # the possibility of silently ignoring errors.
          def raise_exception(failed_row_with_error):
            raise RuntimeError(failed_row_with_error.error_message)

          _ = write_result.failed_rows_with_errors | beam.Map(raise_exception)
          return {
              'post_write': write_result.failed_rows_with_errors
              | beam.FlatMap(lambda x: None)
          }

  return WriteToBigQueryHandlingErrors()


def io_providers():
  with open(os.path.join(os.path.dirname(__file__), 'standard_io.yaml')) as fin:
    return yaml_provider.parse_providers(yaml.load(fin, Loader=yaml.SafeLoader))
