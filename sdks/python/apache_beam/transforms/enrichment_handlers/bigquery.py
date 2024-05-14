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
from typing import Any
from typing import Callable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

from apache_beam.pvalue import Row
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

QueryFn = Callable[[beam.Row], str]
ConditionValueFn = Callable[[beam.Row], List[Any]]


class BigQueryEnrichmentHandler(EnrichmentSourceHandler[Union[Row, List[Row]],
                                                        Union[Row, List[Row]]]):
  def __init__(
      self,
      project: str,
      *,
      table_name: Optional[str] = "",
      row_restriction_template: Optional[str] = "",
      fields: Optional[List[str]] = None,
      column_names: Optional[List[str]] = None,
      condition_value_fn: Optional[ConditionValueFn] = None,
      query_fn: Optional[QueryFn] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      **kwargs,
  ):
    """BigQuery handler for
    :class:`apache_beam.transforms.enrichment.Enrichment` transform.

    Example Usage::
      handler = BigQueryEnrichmentHandler(project=project_name,
        row_restriction="id='{}'",

        table_name='project.dataset.table',

        fields=fields,

        min_batch_size=2,

        max_batch_size=100,
      )

    Args:
      project: Google Cloud project ID for the BigQuery table.
      table_name (str): Fully qualified BigQuery table name
        in the format `project.dataset.table`.
      row_restriction_template (str): A template string for the `WHERE` clause
        in the BigQuery query with placeholders to dynamically filter rows
        based on input data.
      fields: (Optional[List[str]]) List of field names present in the input
        `beam.Row`. These are used to construct the WHERE clause
        (if `condition_value_fn` is not provided).
      column_names: (Optional[List[str]]) Names of columns to select from the
        BigQuery table. If not provided, all columns (`*`) are selected.
      condition_value_fn: (Optional[Callable[[beam.Row], str]]) A function
        that takes a `beam.Row` and returns a string value to be used in the
        `WHERE` clause of the BigQuery query.
      query_fn: (Optional[Callable[[beam.Row], str]]) A function that takes a
        `beam.Row` and returns a complete BigQuery SQL query string.
        If provided, it overrides the default query construction that use
        `fields`, `column_names`, and `condition_value_fn`.
      min_batch_size: (Optional[int]) Minimum number of rows to batch together
        when querying BigQuery.
      max_batch_size: (Optional[int]) Maximum number of rows to batch together.
      **kwargs: Additional keyword arguments to pass to `bigquery.Client`.

    Note:
      * `min_batch_size` and `max_batch_size` won't have any effect if the
        `query_fn` is provided.
      * Either `fields` or `condition_value_fn` must be provided for query
        construction if `query_fn` is not provided.
      * If `query_fn` is provided, it overrides the default query construction.
      * Ensure appropriate permissions are granted for BigQuery access.
    """
    self.project = project
    self.column_names = column_names
    self.select_fields = ",".join(column_names) if column_names else '*'
    self.row_restriction_template = row_restriction_template
    self.table_name = table_name
    self.fields = fields
    self.condition_value_fn = condition_value_fn
    self.query_fn = query_fn
    self.query_template = (
        "SELECT %s FROM %s WHERE %s" %
        (self.select_fields, self.table_name, self.row_restriction_template))
    self.kwargs = kwargs
    self._batching_kwargs = {}
    if not query_fn:
      if min_batch_size is not None:
        self._batching_kwargs['min_batch_size'] = min_batch_size
      if max_batch_size is not None:
        self._batching_kwargs['max_batch_size'] = max_batch_size

  def __enter__(self):
    self.client = bigquery.Client(project=self.project, **self.kwargs)

  def _execute_query(self, query: str):
    try:
      results = self.client.query(query=query).result()
      if self._batching_kwargs:
        return [dict(row.items()) for row in results]
      else:
        return [dict(row.items()) for row in results][0]
    except RuntimeError:
      raise RuntimeError("Could not complete the query request: %s" % query)

  def __call__(self, request: Union[beam.Row, List[beam.Row]], *args, **kwargs):
    # raise ValueError(type(request))
    if isinstance(request, List):
      values = []
      responses = []
      requests_map = {}
      batch_size = len(request)
      raw_query = self.query_template
      if batch_size > 1:
        batched_condition_template = ' or '.join(
            [self.row_restriction_template] * batch_size)
        raw_query = self.query_template.replace(
            self.row_restriction_template, batched_condition_template)
      for req in request:
        request_dict = req._asdict()
        current_values = (
            self.condition_value_fn(req) if self.condition_value_fn else
            [request_dict.get(field) for field in self.fields])
        values.extend(current_values)
        requests_map.update((val, req) for val in current_values)
      query = raw_query.format(*values)

      responses_dict = self._execute_query(query)
      for response in responses_dict:
        for value in response.values():
          if value in requests_map:
            responses.append((requests_map[value], beam.Row(**response)))
      return responses
    else:
      request_dict = request._asdict()
      if self.query_fn:
        # if a query_fn is provided then it return a list of values
        # that should be populated into the query template string.
        query = self.query_fn(request)
      else:
        values = (
            self.condition_value_fn(request) if self.condition_value_fn else
            list(map(request_dict.get, self.fields)))
        # construct the query.
        query = self.query_template.format(*values)
      response_dict = self._execute_query(query)
      return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def get_cache_key(
      self, request: Union[beam.Row, List[beam.Row]]) -> Union[str, List[str]]:
    key = ";".join(["%s"] * len(self.fields))
    if self._batching_kwargs:
      cache_keys = []
      for req in request:
        req_dict = req._asdict()
        key = ";".join(["%s"] * len(self.fields))
        cache_keys.extend([key % req_dict[field] for field in self.fields])
        return cache_keys
    else:
      req_dict = request._asdict()
      return key % (req_dict.get(field) for field in self.fields)

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns a kwargs suitable for `beam.BatchElements`."""
    return self._batching_kwargs
