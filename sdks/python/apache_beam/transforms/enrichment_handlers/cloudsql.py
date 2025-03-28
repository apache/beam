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
from collections.abc import Callable
from enum import Enum
from typing import Any
from typing import Optional

from google.cloud.sql.connector import Connector

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel

__all__ = [
    'CloudSQLEnrichmentHandler',
]

# RowKeyFn takes beam.Row and returns tuple of (key_id, key_value).
RowKeyFn = Callable[[beam.Row], tuple[str]]

_LOGGER = logging.getLogger(__name__)


class DatabaseTypeAdapter(Enum):
  POSTGRESQL = "pg8000"
  MYSQL = "pymysql"
  SQLSERVER = "pytds"


class CloudSQLEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """A handler for :class:`apache_beam.transforms.enrichment.Enrichment`
  transform to interact with Google Cloud SQL databases.

  Args:
    project_id (str): GCP project-id of the Cloud SQL instance.
    region_id (str): GCP region-id of the Cloud SQL instance.
    instance_id (str): GCP instance-id of the Cloud SQL instance.
    database_type_adapter (DatabaseTypeAdapter): The type of database adapter to use.
      Supported adapters are: POSTGRESQL (pg8000), MYSQL (pymysql), and SQLSERVER (pytds).
    database_id (str): The id of the database to connect to.
    database_user (str): The username for connecting to the database.
    database_password (str): The password for connecting to the database.
    table_id (str): The name of the table to query.
    row_key (str): Field name from the input `beam.Row` object to use as 
      identifier for database querying.
    row_key_fn: A lambda function that returns a string key from the
      input row. Used to build/extract the identifier for the database query.
    exception_level: A `enum.Enum` value from
      ``apache_beam.transforms.enrichment_handlers.utils.ExceptionLevel``
      to set the level when no matching record is found from the database query.
      Defaults to ``ExceptionLevel.WARN``.
  """
  def __init__(
      self,
      region_id: str,
      project_id: str,
      instance_id: str,
      database_type_adapter: DatabaseTypeAdapter,
      database_id: str,
      database_user: str,
      database_password: str,
      table_id: str,
      row_key: str = "",
      *,
      row_key_fn: Optional[RowKeyFn] = None,
      exception_level: ExceptionLevel = ExceptionLevel.WARN,
  ):
    self._project_id = project_id
    self._region_id = region_id
    self._instance_id = instance_id
    self._database_type_adapter = database_type_adapter
    self._database_id = database_id
    self._database_user = database_user
    self._database_password = database_password
    self._table_id = table_id
    self._row_key = row_key
    self._row_key_fn = row_key_fn
    self._exception_level = exception_level
    if ((not self._row_key_fn and not self._row_key) or
        bool(self._row_key_fn and self._row_key)):
      raise ValueError(
          "Please specify exactly one of `row_key` or a lambda "
          "function with `row_key_fn` to extract the row key "
          "from the input row.")

  def __enter__(self):
    """Connect to the the Cloud SQL instance."""
    self.connector = Connector()
    self.client = self.connector.connect(
        f"{self._project_id}:{self._region_id}:{self._instance_id}",
        driver=self._database_type_adapter.value,
        db=self._database_id,
        user=self._database_user,
        password=self._database_password,
    )
    self.cursor = self.client.cursor()

  def __call__(self, request: beam.Row, *args, **kwargs):
    """
    Executes a query to the Cloud SQL instance and returns
    a `Tuple` of request and response.

    Args:
    request: the input `beam.Row` to enrich.
    """
    response_dict: dict[str, Any] = {}
    row_key_str: str = ""

    try:
      if self._row_key_fn:
        self._row_key, row_key = self._row_key_fn(request)
      else:
        request_dict = request._asdict()
        row_key_str = str(request_dict[self._row_key])
        row_key = row_key_str

      query = f"SELECT * FROM {self._table_id} WHERE {self._row_key} = %s"
      self.cursor.execute(query, (row_key, ))
      result = self.cursor.fetchone()

      if result:
        columns = [col[0] for col in self.cursor.description]
        for i, value in enumerate(result):
          response_dict[columns[i]] = value
      elif self._exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            'No matching record found for row_key: %s in table: %s',
            row_key_str,
            self._table_id)
      elif self._exception_level == ExceptionLevel.RAISE:
        raise ValueError(
            'No matching record found for row_key: %s in table: %s' %
            (row_key_str, self._table_id))
    except KeyError:
      raise KeyError('row_key %s not found in input PCollection.' % row_key_str)
    except Exception as e:
      raise e

    return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Clean the instantiated Cloud SQL client."""
    self.cursor.close()
    self.client.close()
    self.connector.close()
    self.cursor, self.client, self.connector = None, None, None

  def get_cache_key(self, request: beam.Row) -> str:
    """Returns a string formatted with row key since it is unique to
      a request made to the Cloud SQL instance."""
    if self._row_key_fn:
      id, value = self._row_key_fn(request)
      return f"{id}: {value}"
    return f"{self._row_key}: {request._asdict()[self._row_key]}"
