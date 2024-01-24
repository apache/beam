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
import logging
from enum import Enum
from typing import Any
from typing import Dict
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigtable
from google.cloud.bigtable import Client
from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_filters import RowFilter

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = [
    'BigTableEnrichmentHandler',
    'ExceptionLevel',
]

_LOGGER = logging.getLogger(__name__)


class ExceptionLevel(Enum):
  """ExceptionLevel defines the exception level options to either
  log a warning, or raise an exception, or do nothing when a BigTable query
  returns an empty row.

  Members:
    - RAISE: Raise the exception.
    - WARN: Log a warning for exception without raising it.
    - QUIET: Neither log nor raise the exception.
  """
  RAISE = 0
  WARN = 1
  QUIET = 2


class BigTableEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """BigTableEnrichmentHandler is a handler for
  :class:`apache_beam.transforms.enrichment.Enrichment` transform to interact
  with GCP BigTable.

  Args:
    project_id (str): GCP project-id of the BigTable cluster.
    instance_id (str): GCP instance-id of the BigTable cluster.
    table_id (str): GCP table-id of the BigTable.
    row_key (str): unique row-key field name from the input `beam.Row` object
      to use as `row_key` for BigTable querying.
    row_filter: a ``:class:`google.cloud.bigtable.row_filters.RowFilter``` to
      filter data read with ``read_row()``.
      Defaults to `CellsColumnLimitFilter(1)`.
    app_profile_id (str): App profile ID to use for BigTable.
      See https://cloud.google.com/bigtable/docs/app-profiles for more details.
    encoding (str): encoding type to convert the string to bytes and vice-versa
      from BigTable. Default is `utf-8`.
    exception_level: a `enum.Enum` value from
      ``apache_beam.transforms.enrichment_handlers.bigtable.ExceptionLevel``
      to set the level when an empty row is returned from the BigTable query.
      Defaults to ``ExceptionLevel.WARN``.
    include_timestamp (bool): If enabled, the timestamp associated with the
      value is returned as `(value, timestamp)` for each `row_key`.
      Defaults to `False` - only the latest value without
      the timestamp is returned.
  """
  def __init__(
      self,
      project_id: str,
      instance_id: str,
      table_id: str,
      row_key: str,
      row_filter: Optional[RowFilter] = CellsColumnLimitFilter(1),
      *,
      app_profile_id: str = None,  # type: ignore[assignment]
      encoding: str = 'utf-8',
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
      include_timestamp: bool = False,
  ):
    self._project_id = project_id
    self._instance_id = instance_id
    self._table_id = table_id
    self._row_key = row_key
    self._row_filter = row_filter
    self._app_profile_id = app_profile_id
    self._encoding = encoding
    self._exception_level = exception_level
    self._include_timestamp = include_timestamp

  def __enter__(self):
    """connect to the Google BigTable cluster."""
    self.client = Client(project=self._project_id)
    self.instance = self.client.instance(self._instance_id)
    self._table = bigtable.table.Table(
        table_id=self._table_id,
        instance=self.instance,
        app_profile_id=self._app_profile_id)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """
    Reads a row from the GCP BigTable and returns
    a `Tuple` of request and response.

    Args:
    request: the input `beam.Row` to enrich.
    """
    response_dict: Dict[str, Any] = {}
    row_key_str: str = ""
    try:
      request_dict = request._asdict()
      row_key_str = str(request_dict[self._row_key])
      row_key = row_key_str.encode(self._encoding)
      row = self._table.read_row(row_key, filter_=self._row_filter)
      if row:
        for cf_id, cf_v in row.cells.items():
          response_dict[cf_id] = {}
          for col_id, col_v in cf_v.items():
            if self._include_timestamp:
              response_dict[cf_id][col_id.decode(self._encoding)] = [
                  (v.value.decode(self._encoding), v.timestamp) for v in col_v
              ]
            else:
              response_dict[cf_id][col_id.decode(
                  self._encoding)] = col_v[0].value.decode(self._encoding)
      elif self._exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            'no matching row found for row_key: %s '
            'with row_filter: %s' % (row_key_str, self._row_filter))
      elif self._exception_level == ExceptionLevel.RAISE:
        raise ValueError(
            'no matching row found for row_key: %s '
            'with row_filter=%s' % (row_key_str, self._row_filter))
    except KeyError:
      raise KeyError('row_key %s not found in input PCollection.' % row_key_str)
    except NotFound:
      raise NotFound(
          'GCP BigTable cluster `%s:%s:%s` not found.' %
          (self._project_id, self._instance_id, self._table_id))
    except Exception as e:
      raise e

    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated BigTable client."""
    self.client = None
    self.instance = None
    self._table = None
