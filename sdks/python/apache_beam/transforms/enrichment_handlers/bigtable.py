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
from typing import Any
from typing import Dict
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud.bigtable import Client
from google.cloud.bigtable.row_filters import RowFilter

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = [
    'EnrichWithBigTable',
]

_LOGGER = logging.getLogger(__name__)


class EnrichWithBigTable(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """EnrichWithBigTable is a handler for
  :class:`apache_beam.transforms.enrichment.Enrichment` transform to interact
  with GCP BigTable.

  Args:
    project_id (str): GCP project-id of the BigTable cluster.
    instance_id (str): GCP instance-id of the BigTable cluster.
    table_id (str): GCP table-id of the BigTable.
    row_key (str): unique row key for BigTable
    row_filter: a ``:class:`google.cloud.bigtable.row_filters.RowFilter``` to
      filter data read with ``read_row()``.
  """
  def __init__(
      self,
      project_id: str,
      instance_id: str,
      table_id: str,
      row_key: str,
      row_filter: Optional[RowFilter] = None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._table_id = table_id
    self._row_key = row_key
    self._row_filter = row_filter

  def __enter__(self):
    """connect to the Google BigTable cluster."""
    self.client = Client(project=self._project_id)
    self.instance = self.client.instance(self._instance_id)
    self._table = self.instance.table(self._table_id)

  def __call__(self, request: beam.Row, *args, **kwargs):
    """
    Reads a row from the Google BigTable and returns
    a `Tuple` of request and response.

    Args:
    request: the input `beam.Row` to enrich.
    """
    response_dict: Dict[str, Any] = {}
    try:
      request_dict = request._asdict()
      row_key = str(request_dict[self._row_key]).encode()
      row = self._table.read_row(row_key, filter_=self._row_filter)
      if row:
        for cf_id, cf_v in row.cells.items():
          response_dict[cf_id] = {}
          for k, v in cf_v.items():
            response_dict[cf_id][k.decode('utf-8')] = v[0].value.decode('utf-8')
    except NotFound:
      _LOGGER.warning('request row_key: %s not found')
    except Exception as e:
      raise e

    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated BigTable client."""
    self.client = None
    self.instance = None
    self._table = None
