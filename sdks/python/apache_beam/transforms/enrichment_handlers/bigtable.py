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
from typing import List
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigtable

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = [
    'EnrichWithBigTable',
]

_LOGGER = logging.getLogger(__name__)


class EnrichWithBigTable(EnrichmentSourceHandler[dict, beam.Row]):
  """EnrichWithBigTable is a handler for
  :class:`apache_beam.transforms.enrichment.Enrichment` transform to interact
  with GCP BigTable.

  Args:
    project_id (str): GCP project-id of the BigTable cluster.
    instance_id (str): GCP instance-id of the BigTable cluster.
    table_id (str): GCP table-id of the BigTable.
    row_key (str): unique row key for BigTable
    column_family_ids (List(str))
  """
  def __init__(
      self,
      project_id: str,
      instance_id: str,
      table_id: str,
      row_key: str,
      column_family_ids: Optional[List[str]] = None,
      column_ids: Optional[List[str]] = None):
    self._project_id = project_id
    self._instance_id = instance_id
    self._table_id = table_id
    self._row_key = row_key
    self._column_family_ids = column_family_ids
    self._column_ids = column_ids

  def __enter__(self):
    client = bigtable.Client(project=self._project_id)
    instance = client.instance(self._instance_id)
    self._table = instance.table(self._table_id)

  def __call__(self, request: dict, *args, **kwargs):
    row_key = request[self._row_key].encode()
    row = self._table.read_row(row_key)
    response_dict = {}

    try:
      if self._column_family_ids and self._column_ids:
        for column_family_id in self._column_family_ids:
          response_dict[column_family_id] = {}
          for column_id in self._column_ids:
            response_dict[column_family_id][column_id] = row.cells[
                column_family_id][column_id.encode()][0].value.decode('utf-8')
      elif self._column_family_ids:
        for column_family_id in self._column_family_ids:
          response_dict[column_family_id] = {}
          for k, v in row.cells[column_family_id].items():
            response_dict[column_family_id][k.decode(
                'utf-8')] = v[0].value.decode('utf-8')
      elif self._column_ids:
        for cf_id, cf_v in row.cells.items():
          response_dict[cf_id] = {}
          for c_id in self._column_ids:
            if c_id.encode() in cf_v:
              response_dict[cf_id][c_id] = cf_v[c_id.encode()][0].value.decode(
                  'utf-8')
      else:
        for cf_id, cf_v in row.cells.items():
          response_dict[cf_id] = {}
          for k, v in cf_v.items():
            response_dict[cf_id][k.decode('utf-8')] = v[0].value.decode('utf-8')
    except NotFound:
      _LOGGER.warning('request row_key: %s not found')
    except Exception as e:
      raise e

    return request, response_dict

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._table = None
