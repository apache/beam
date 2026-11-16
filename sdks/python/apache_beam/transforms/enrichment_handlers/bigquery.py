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
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
import datetime
import decimal
import logging
import re
from typing import Any
from typing import Optional
from typing import Union

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.pvalue import Row
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

QueryFn = Callable[[beam.Row], Union[str, tuple[str, Sequence[Any]]]]
ConditionValueFn = Callable[[beam.Row], list[Any]]

_LOGGER = logging.getLogger(__name__)

_TABLE_NAME_PATTERN = re.compile(
    r'^`?[A-Za-z0-9_.:$-]+`?(?:\.`?[A-Za-z0-9_.:$-]+`?)*$')
_COLUMN_NAME_PATTERN = re.compile(
    r'^(?:\*|`?[A-Za-z_][A-Za-z0-9_]*`?(?:\.`?[A-Za-z_][A-Za-z0-9_]*`?)*)$')
_TEMPLATE_PLACEHOLDER_PATTERN = re.compile(
    r"'\{\s*([^{}'\"]*?)\s*\}'|"
    r'"\{\s*([^{}\'"]*?)\s*\}"|'
    r'\{\s*([^{}\'"]*?)\s*\}|'
    r'(\?)')


def _validate_bigquery_identifier(
    identifier: str, param_name: str, pattern: re.Pattern[str]) -> None:
  if (not identifier or '--' in identifier or '/*' in identifier or
      '*/' in identifier or not pattern.match(identifier)):
    raise ValueError(f"Invalid BigQuery {param_name}: {identifier!r}")


def _parse_restriction_template(
    row_restriction_template: str
) -> tuple[str, list[Optional[Union[int, str]]]]:
  """Normalizes `{}` / `'{}'` / `?` placeholders into `?` positional parameters.

  Returns:
    A tuple of `(normalized_sql_template, placeholder_specs)` where each entry
    in `placeholder_specs` is `None` (next sequential value), an `int` index,
    or a `str` field name.
  """
  specs: list[Optional[Union[int, str]]] = []
  seq_idx = 0

  def _replace(match: re.Match[str]) -> str:
    nonlocal seq_idx
    q_single, q_double, unquoted, qmark = match.groups()
    if qmark is not None:
      specs.append(seq_idx)
      seq_idx += 1
      return '?'
    raw_spec = (
        q_single if q_single is not None else
        (q_double if q_double is not None else unquoted))
    spec = (raw_spec or '').strip()
    if not spec:
      specs.append(seq_idx)
      seq_idx += 1
    elif spec.isdigit():
      specs.append(int(spec))
    else:
      specs.append(spec)
    return '?'

  normalized = _TEMPLATE_PLACEHOLDER_PATTERN.sub(
      _replace, row_restriction_template)
  return normalized, specs


def _infer_bq_scalar_type(value: Any) -> str:
  if isinstance(value, bool):
    return 'BOOL'
  if isinstance(value, int):
    return 'INT64'
  if isinstance(value, float):
    return 'FLOAT64'
  if isinstance(value, decimal.Decimal):
    return 'NUMERIC'
  if isinstance(value, (bytes, bytearray)):
    return 'BYTES'
  if isinstance(value, datetime.datetime):
    return 'TIMESTAMP' if value.tzinfo is not None else 'DATETIME'
  if isinstance(value, datetime.date):
    return 'DATE'
  if isinstance(value, datetime.time):
    return 'TIME'
  return 'STRING'


def _to_query_parameter(
    value: Any,
    name: Optional[str] = None
) -> Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]:
  if hasattr(value, 'to_api_repr'):
    return value
  if isinstance(value, (list, tuple)):
    non_null = next((item for item in value if item is not None), None)
    elem_type = (
        _infer_bq_scalar_type(non_null) if non_null is not None else 'STRING')
    return bigquery.ArrayQueryParameter(name, elem_type, list(value))
  param_type = _infer_bq_scalar_type(value)
  if param_type == 'STRING' and value is not None and not isinstance(value,
                                                                     str):
    value = str(value)
  return bigquery.ScalarQueryParameter(name, param_type, value)


def _validate_bigquery_metadata(
    table_name,
    row_restriction_template,
    fields,
    column_names,
    condition_value_fn,
    query_fn):
  if query_fn:
    if bool(table_name or row_restriction_template or fields or
            condition_value_fn):
      raise ValueError(
          "Please provide either `query_fn` or the parameters `table_name`, "
          "`row_restriction_template`, and `fields/condition_value_fn` "
          "together.")
  else:
    if not (table_name and row_restriction_template):
      raise ValueError(
          "Please provide either `query_fn` or the parameters "
          "`table_name`, `row_restriction_template` together.")
    if ((fields and condition_value_fn) or
        (not fields and not condition_value_fn)):
      raise ValueError(
          "Please provide exactly one of `fields` or "
          "`condition_value_fn`")
    _validate_bigquery_identifier(table_name, 'table_name', _TABLE_NAME_PATTERN)
  if column_names:
    for col in column_names:
      _validate_bigquery_identifier(col, 'column_name', _COLUMN_NAME_PATTERN)


class BigQueryEnrichmentHandler(EnrichmentSourceHandler[Union[Row, list[Row]],
                                                        Union[Row, list[Row]]]):
  """Enrichment handler for Google Cloud BigQuery.

  Use this handler with :class:`apache_beam.transforms.enrichment.Enrichment`
  transform.

  To use this handler you need either of the following combinations:
    * `table_name`, `row_restriction_template`, `fields`
    * `table_name`, `row_restriction_template`, `condition_value_fn`
    * `query_fn`

  By default, the handler pulls all columns from the BigQuery table.
  To override this, use the `column_name` parameter to specify a list of column
  names to fetch.

  This handler pulls data from BigQuery per element by default. To change this
  behavior, set the `min_batch_size`, `max_batch_size`, and
  `max_batch_duration_secs` parameters.
  These batching values are sent to the
  :class:`apache_beam.transforms.utils.BatchElements` transform.

  NOTE: Elements cannot be batched when using the `query_fn` parameter.
  """
  def __init__(
      self,
      project: str,
      *,
      table_name: str = "",
      row_restriction_template: str = "",
      fields: Optional[list[str]] = None,
      column_names: Optional[list[str]] = None,
      condition_value_fn: Optional[ConditionValueFn] = None,
      query_fn: Optional[QueryFn] = None,
      min_batch_size: int = 1,
      max_batch_size: int = 10000,
      max_batch_duration_secs: Optional[float] = None,
      throw_exception_on_empty_results: bool = True,
      **kwargs,
  ):
    """
    Example Usage:
      handler = BigQueryEnrichmentHandler(project=project_name,
                                          row_restriction="id='{}'",
                                          table_name='project.dataset.table',
                                          fields=fields,
                                          min_batch_size=2,
                                          max_batch_size=100)

    Args:
      project: Google Cloud project ID for the BigQuery table.
      table_name (str): Fully qualified BigQuery table name
        in the format `project.dataset.table`.
      row_restriction_template (str): A template string for the `WHERE` clause
        in the BigQuery query with placeholders (`{}`) to dynamically filter
        rows based on input data.
      fields: (Optional[list[str]]) List of field names present in the input
        `beam.Row`. These are used to construct the WHERE clause
        (if `condition_value_fn` is not provided).
      column_names: (Optional[list[str]]) Names of columns to select from the
        BigQuery table. If not provided, all columns (`*`) are selected.
      condition_value_fn: (Optional[Callable[[beam.Row], Any]]) A function
        that takes a `beam.Row` and returns a list of value to populate in the
        placeholder `{}` of `WHERE` clause in the query.
      query_fn: (Optional[Callable[[beam.Row], str]]) A function that takes a
        `beam.Row` and returns a complete BigQuery SQL query string.
      min_batch_size (int): Minimum number of rows to batch together when
        querying BigQuery. Defaults to 1 if `query_fn` is not specified.
      max_batch_size (int): Maximum number of rows to batch together.
        Defaults to 10,000 if `query_fn` is not specified.
      max_batch_duration_secs (float): Maximum amount of time in seconds to
        buffer a batch before emitting it. If not provided, batching duration
        is determined by `BatchElements` defaults.
      **kwargs: Additional keyword arguments to pass to `bigquery.Client`.

    Note:
      * `min_batch_size`, `max_batch_size`, and `max_batch_duration_secs`
        are not used if `query_fn` is provided.
      * Either `fields` or `condition_value_fn` must be provided for query
        construction if `query_fn` is not provided.
      * Ensure appropriate permissions are granted for BigQuery access.
    """
    _validate_bigquery_metadata(
        table_name,
        row_restriction_template,
        fields,
        column_names,
        condition_value_fn,
        query_fn)
    self.project = project
    self.column_names = column_names
    self.select_fields = ",".join(column_names) if column_names else '*'
    self.row_restriction_template = row_restriction_template
    (
        self._normalized_row_restriction_template,
        self._placeholder_specs,
    ) = _parse_restriction_template(row_restriction_template)
    self.table_name = table_name
    self.fields = fields if fields else []
    self.condition_value_fn = condition_value_fn
    self.query_fn = query_fn
    self.query_template = (
        "SELECT %s FROM %s WHERE %s" % (
            self.select_fields,
            self.table_name,
            self._normalized_row_restriction_template))
    self.throw_exception_on_empty_results = throw_exception_on_empty_results
    self.kwargs = kwargs
    self._batching_kwargs = {}
    if not query_fn:
      self._batching_kwargs['min_batch_size'] = min_batch_size
      self._batching_kwargs['max_batch_size'] = max_batch_size
      if max_batch_duration_secs is not None:
        self._batching_kwargs[
            'max_batch_duration_secs'] = max_batch_duration_secs

  def __enter__(self):
    self.client = bigquery.Client(project=self.project, **self.kwargs)

  def _build_row_parameters(
      self,
      current_values: Sequence[Any],
      request_dict: Optional[Mapping[str, Any]] = None,
  ) -> list[Union[bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter]]:
    if not self._placeholder_specs:
      return [_to_query_parameter(v) for v in current_values]
    params: list[Union[bigquery.ScalarQueryParameter,
                       bigquery.ArrayQueryParameter]] = []
    for pos, spec in enumerate(self._placeholder_specs):
      if isinstance(spec, int):
        if spec >= len(current_values):
          raise ValueError(
              f"Placeholder index {spec} out of range for "
              f"{len(current_values)} condition value(s).")
        val = current_values[spec]
      elif isinstance(spec, str):
        if request_dict is not None and spec in request_dict:
          val = request_dict[spec]
        elif spec in self.fields:
          val = current_values[self.fields.index(spec)]
        elif pos < len(current_values):
          val = current_values[pos]
        else:
          raise KeyError(
              f"Placeholder field {spec!r} not found in input `beam.Row`.")
      else:
        val = current_values[pos]
      params.append(_to_query_parameter(val))
    return params

  def _execute_query(
      self, query: str, query_parameters: Optional[Sequence[Any]] = None):
    try:
      if query_parameters is not None:
        job_config = bigquery.QueryJobConfig(
            query_parameters=list(query_parameters))
        results = self.client.query(query=query, job_config=job_config).result()
      else:
        results = self.client.query(query=query).result()
      row_list = [dict(row.items()) for row in results]
      if not row_list:
        return None
      if self._batching_kwargs:
        return row_list
      else:
        return row_list[0]
    except BadRequest as e:
      raise BadRequest(
          f'Could not execute the query: {query}. Please check if '
          f'the query is properly formatted and the BigQuery '
          f'table exists. {e}')
    except RuntimeError as e:
      raise RuntimeError(f"Could not complete the query request: {query}. {e}")

  def create_row_key(self, row: beam.Row):
    if self.condition_value_fn:
      return tuple(self.condition_value_fn(row))
    if self.fields:
      row_dict = row._asdict()
      return (tuple(row_dict[field] for field in self.fields))
    raise ValueError("Either fields or condition_value_fn must be specified")

  def __call__(self, request: Union[beam.Row, list[beam.Row]], *args, **kwargs):
    if isinstance(request, list):
      query_parameters: list[Any] = []
      responses = []
      requests_map: dict[Any, list[beam.Row]] = defaultdict(list)
      batch_size = len(request)
      query = self.query_template
      if batch_size > 1:
        batched_condition_template = ' or '.join(
            [f'({self._normalized_row_restriction_template})'] * batch_size)
        query = (
            "SELECT %s FROM %s WHERE %s" %
            (self.select_fields, self.table_name, batched_condition_template))
      for req in request:
        request_dict = req._asdict()
        try:
          current_values = (
              self.condition_value_fn(req) if self.condition_value_fn else
              [request_dict[field] for field in self.fields])
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `fields` are the "
              "keys in the input `beam.Row`." + str(e))
        query_parameters.extend(
            self._build_row_parameters(current_values, request_dict))
        requests_map[self.create_row_key(req)].append(req)

      responses_dict = self._execute_query(query, query_parameters)
      unmatched_requests = {
          key: list(reqs)
          for key, reqs in requests_map.items()
      }
      if responses_dict:
        for response in responses_dict:
          response_row = beam.Row(**response)
          response_key = self.create_row_key(response_row)
          if response_key in unmatched_requests:
            for req in unmatched_requests.pop(response_key):
              responses.append((req, response_row))
      if unmatched_requests:
        if self.throw_exception_on_empty_results:
          raise ValueError(f"no matching row found for query: {query}")
        else:
          _LOGGER.warning('no matching row found for query: %s', query)
          for reqs in unmatched_requests.values():
            for req in reqs:
              responses.append((req, beam.Row()))
      return responses
    else:
      request_dict = request._asdict()
      single_query_parameters: Optional[list[Any]] = None
      if self.query_fn:
        query_result = self.query_fn(request)
        if isinstance(query_result, tuple):
          query, raw_params = query_result
          single_query_parameters = [_to_query_parameter(v) for v in raw_params]
        else:
          query = query_result
      else:
        values = (
            self.condition_value_fn(request) if self.condition_value_fn else
            list(map(request_dict.get, self.fields)))
        query = self.query_template
        single_query_parameters = self._build_row_parameters(
            values, request_dict)
      response_dict = self._execute_query(query, single_query_parameters)
      if response_dict is None:
        if self.throw_exception_on_empty_results:
          raise ValueError(f"no matching row found for query: {query}")
        else:
          _LOGGER.warning('no matching row found for query: %s', query)
        return request, beam.Row()
      if isinstance(response_dict, list):
        response_dict = response_dict[0]
      return request, beam.Row(**response_dict)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.client.close()

  def get_cache_key(self, request: Union[beam.Row, list[beam.Row]]):
    if isinstance(request, list):
      cache_keys = []
      for req in request:
        req_dict = req._asdict()
        try:
          current_values = (
              self.condition_value_fn(req) if self.condition_value_fn else
              [req_dict[field] for field in self.fields])
          key = ";".join(["%s"] * len(current_values))
          cache_keys.extend([key % tuple(current_values)])
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `fields` are the "
              "keys in the input `beam.Row`." + str(e))
      return cache_keys
    else:
      req_dict = request._asdict()
      try:
        current_values = (
            self.condition_value_fn(request) if self.condition_value_fn else
            [req_dict[field] for field in self.fields])
        key = ";".join(["%s"] * len(current_values))
        cache_key = key % tuple(current_values)
      except KeyError as e:
        raise KeyError(
            "Make sure the values passed in `fields` are the "
            "keys in the input `beam.Row`." + str(e))
      return cache_key

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns a kwargs suitable for `beam.BatchElements`."""
    return self._batching_kwargs
