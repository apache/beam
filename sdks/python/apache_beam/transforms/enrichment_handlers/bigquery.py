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
import copy
import datetime
import decimal
import logging
import re
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import Union

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.pvalue import Row
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

__all__ = ['BigQueryEnrichmentHandler']

QueryFn = Callable[[beam.Row], Union[str, tuple[str, Sequence[Any]]]]
ConditionValueFn = Callable[[beam.Row], list[Any]]
PlaceholderRenderer = Callable[[str, str, bool], str]

_LOGGER = logging.getLogger(__name__)

MAX_BQ_QUERY_PARAMETERS = 10000

SIMPLE_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_SIMPLE_IDENTIFIER_PREFIX = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
_SYS_VAR_PREFIX = re.compile(r'[A-Za-z_][A-Za-z0-9_.]*')
_TABLE_SEGMENT = (
    r'(?:`(?:[A-Za-z0-9_:$-]+\.)*[A-Za-z0-9_:$-]+`|[A-Za-z0-9_:$-]+)')
TABLE_NAME_PATTERN = re.compile(rf'^{_TABLE_SEGMENT}(?:\.{_TABLE_SEGMENT})*$')
_COLUMN_SEGMENT = r'(?:`[A-Za-z_][A-Za-z0-9_]*`|[A-Za-z_][A-Za-z0-9_]*)'
COLUMN_NAME_PATTERN = re.compile(
    rf'^(?:\*|{_COLUMN_SEGMENT}(?:\.{_COLUMN_SEGMENT})*)$')


def validate_bigquery_identifier(
    identifier: str, param_name: str, pattern: re.Pattern[str]) -> None:
  if (not isinstance(identifier, str) or not identifier or '--' in identifier or
      '/*' in identifier or '*/' in identifier or
      not pattern.match(identifier)):
    raise ValueError(f"Invalid BigQuery {param_name}: {identifier!r}")


def _validate_brace_spec(raw_spec: str, sql: str) -> str:
  spec = raw_spec.strip()
  if spec == '' or spec.isdigit() or SIMPLE_IDENTIFIER_PATTERN.match(spec):
    return spec
  raise ValueError(
      f"Invalid placeholder {{{raw_spec}}} in SQL template: {sql!r}")


def _find_closing_quote(sql: str, start: int, quote: str) -> int:
  n = len(sql)
  j = start
  while j < n:
    if sql.startswith(quote, j):
      return j
    if sql[j] == '\\':
      if j + 1 >= n:
        raise ValueError(f"Unterminated escape in SQL string literal: {sql!r}")
      j += 2
      continue
    j += 1
  raise ValueError(f"Unterminated string literal in SQL template: {sql!r}")


def scan_sql_template(sql: str, render_placeholder: PlaceholderRenderer) -> str:
  """Scans a GoogleSQL template while respecting strings, comments, and quotes.

  Calls ``render_placeholder(kind, spec, in_string)`` for each placeholder:
    * ``kind == 'brace'`` for ``{}``, ``{0}``, or ``{field}`` (both unquoted and
      inside plain single- or double-quoted SQL string literals).
    * ``kind == 'qmark'`` for unquoted ``?`` positional parameters.
    * ``kind == 'at'`` for unquoted ``@param`` named parameters (excluding
      ``@@system_variables``).
  """
  n = len(sql)
  i = 0
  out: list[str] = []

  while i < n:
    if sql.startswith('--', i):
      nl = sql.find('\n', i + 2)
      if nl == -1:
        out.append(sql[i:])
        break
      out.append(sql[i:nl + 1])
      i = nl + 1
      continue

    if sql[i] == '#':
      nl = sql.find('\n', i + 1)
      if nl == -1:
        out.append(sql[i:])
        break
      out.append(sql[i:nl + 1])
      i = nl + 1
      continue

    if sql.startswith('/*', i):
      end = sql.find('*/', i + 2)
      if end == -1:
        raise ValueError(f"Unterminated block comment in SQL template: {sql!r}")
      out.append(sql[i:end + 2])
      i = end + 2
      continue

    if sql[i] == '`':
      j = i + 1
      while j < n and sql[j] != '`':
        if sql[j] == '\\' and j + 1 < n:
          j += 2
        else:
          j += 1
      if j >= n:
        raise ValueError(
            f"Unterminated backtick identifier in SQL template: {sql!r}")
      out.append(sql[i:j + 1])
      i = j + 1
      continue

    prev_is_ident = i > 0 and (sql[i - 1].isalnum() or sql[i - 1] == '_')
    if not prev_is_ident:
      two_prefix = sql[i:i + 2].lower()
      prefix_len = (
          2 if two_prefix in ('rb', 'br') else
          (1 if sql[i].lower() in ('r', 'b') else 0))
      if (prefix_len > 0 and i + prefix_len < n and
          sql[i + prefix_len] in ("'", '"')):
        q_start = i + prefix_len
        q_char = sql[q_start]
        quote = q_char * 3 if sql.startswith(q_char * 3, q_start) else q_char
        end_q = _find_closing_quote(sql, q_start + len(quote), quote)
        prefix = sql[i:q_start]
        body = sql[q_start + len(quote):end_q]
        unescaped_body = body.replace('{{', '{').replace('}}', '}')
        out.append(f"{prefix}{quote}{unescaped_body}{quote}")
        i = end_q + len(quote)
        continue

    if sql[i] in ("'", '"'):
      q_char = sql[i]
      quote = q_char * 3 if sql.startswith(q_char * 3, i) else q_char
      end_q = _find_closing_quote(sql, i + len(quote), quote)
      body = sql[i + len(quote):end_q]
      i = end_q + len(quote)

      if '{' not in body and '}' not in body:
        out.append(f"{quote}{body}{quote}")
        continue

      segments: list[str] = []
      placeholders: list[str] = []
      buf: list[str] = []
      k = 0
      m = len(body)
      while k < m:
        if body[k] == '\\' and k + 1 < m:
          buf.append(body[k:k + 2])
          k += 2
        elif body.startswith('{{', k):
          buf.append('{')
          k += 2
        elif body.startswith('}}', k):
          buf.append('}')
          k += 2
        elif body[k] == '}':
          raise ValueError(f"Single '}}' encountered in SQL template: {sql!r}")
        elif body[k] == '{':
          close_k = body.find('}', k + 1)
          if close_k == -1:
            raise ValueError(f"Unclosed '{{' in SQL template: {sql!r}")
          raw_spec = body[k + 1:close_k]
          if '{' in raw_spec:
            raise ValueError(
                f"Nested '{{' in SQL template placeholder: {sql!r}")
          spec = _validate_brace_spec(raw_spec, sql)
          segments.append(''.join(buf))
          buf = []
          placeholders.append(spec)
          k = close_k + 1
        else:
          buf.append(body[k])
          k += 1
      segments.append(''.join(buf))

      if not placeholders:
        out.append(f"{quote}{segments[0]}{quote}")
      elif len(placeholders) == 1 and segments[0] == '' and segments[1] == '':
        out.append(render_placeholder('brace', placeholders[0], False))
      else:
        concat_parts: list[str] = []
        for idx, ph_spec in enumerate(placeholders):
          if segments[idx]:
            concat_parts.append(f"{quote}{segments[idx]}{quote}")
          rendered = render_placeholder('brace', ph_spec, True)
          concat_parts.append(f"CAST({rendered} AS STRING)")
        if segments[-1]:
          concat_parts.append(f"{quote}{segments[-1]}{quote}")
        out.append(f"CONCAT({', '.join(concat_parts)})")
      continue

    if sql.startswith('{{', i):
      out.append('{')
      i += 2
      continue

    if sql.startswith('}}', i):
      out.append('}')
      i += 2
      continue

    if sql[i] == '}':
      raise ValueError(f"Single '}}' encountered in SQL template: {sql!r}")

    if sql[i] == '{':
      close_i = sql.find('}', i + 1)
      if close_i == -1:
        raise ValueError(f"Unclosed '{{' in SQL template: {sql!r}")
      raw_spec = sql[i + 1:close_i]
      if '{' in raw_spec:
        raise ValueError(f"Nested '{{' in SQL template placeholder: {sql!r}")
      spec = _validate_brace_spec(raw_spec, sql)
      out.append(render_placeholder('brace', spec, False))
      i = close_i + 1
      continue

    if sql[i] == '?':
      out.append(render_placeholder('qmark', '', False))
      i += 1
      continue

    if sql.startswith('@@', i):
      sys_match = _SYS_VAR_PREFIX.match(sql, i + 2)
      if not sys_match:
        raise ValueError(
            f"Invalid system variable syntax in SQL template: {sql!r}")
      out.append(sql[i:sys_match.end()])
      i = sys_match.end()
      continue

    if sql[i] == '@':
      at_match = _SIMPLE_IDENTIFIER_PREFIX.match(sql, i + 1)
      if not at_match:
        raise ValueError(
            f"Invalid '@' parameter syntax in SQL template: {sql!r}")
      out.append(render_placeholder('at', at_match.group(0), False))
      i = at_match.end()
      continue

    out.append(sql[i])
    i += 1

  return ''.join(out)


def _parse_restriction_template(
    row_restriction_template: str) -> tuple[str, list[Union[int, str]]]:
  """Normalizes placeholders into ``?`` positional BigQuery parameters.

  Returns:
    A tuple of ``(normalized_sql_template, placeholder_specs)`` where each entry
    in ``placeholder_specs`` is an ``int`` positional index or a ``str`` field
    name.
  """
  if not row_restriction_template:
    return '', []

  specs: list[Union[int, str]] = []
  seq_idx = 0
  has_auto = False
  has_manual = False

  def _render(kind: str, spec: str, in_string: bool) -> str:
    del in_string
    nonlocal seq_idx, has_auto, has_manual
    if kind == 'qmark' or (kind == 'brace' and spec == ''):
      has_auto = True
      if has_manual:
        raise ValueError(
            "Cannot mix automatic positional placeholders ('{}' or '?') with "
            "numbered or named placeholders in row_restriction_template.")
      specs.append(seq_idx)
      seq_idx += 1
      return '?'

    has_manual = True
    if has_auto:
      raise ValueError(
          "Cannot mix automatic positional placeholders ('{}' or '?') with "
          "numbered or named placeholders in row_restriction_template.")
    if kind == 'brace' and spec.isdigit():
      specs.append(int(spec))
    else:
      specs.append(spec)
    return '?'

  normalized = scan_sql_template(row_restriction_template, _render)
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


def to_query_parameter(
    value: Any,
    name: Optional[str] = None,
    fallback_type: Optional[str] = None,
) -> Any:
  """Converts a Python value into a BigQuery query parameter."""
  if hasattr(value, 'to_api_repr'):
    if name is not None and getattr(value, 'name', None) != name:
      param_copy = copy.copy(value)
      param_copy.name = name
      return param_copy
    return value
  if isinstance(value, (list, tuple)):
    non_null = next((item for item in value if item is not None), None)
    elem_type = (
        _infer_bq_scalar_type(non_null) if non_null is not None else
        (fallback_type or 'STRING'))
    return bigquery.ArrayQueryParameter(name, elem_type, list(value))
  if value is None:
    return bigquery.ScalarQueryParameter(name, fallback_type or 'STRING', None)
  param_type = _infer_bq_scalar_type(value)
  if param_type == 'STRING' and not isinstance(value, str):
    value = str(value)
  return bigquery.ScalarQueryParameter(name, param_type, value)


def _make_hashable(value: Any) -> Any:
  if hasattr(value, 'to_api_repr'):
    if hasattr(value, 'value'):
      return _make_hashable(value.value)
    if hasattr(value, 'values'):
      return _make_hashable(value.values)
    return repr(value.to_api_repr())
  if isinstance(value, (list, tuple)):
    return tuple(_make_hashable(v) for v in value)
  if isinstance(value, dict):
    return tuple(sorted((k, _make_hashable(v)) for k, v in value.items()))
  if isinstance(value, bytearray):
    return bytes(value)
  return value


def _infer_slot_fallback_types(
    all_row_slot_values: Sequence[Sequence[Any]],
    num_slots: int,
) -> list[Optional[str]]:
  fallback_types: list[Optional[str]] = [None] * num_slots
  for slot_idx in range(num_slots):
    for row_vals in all_row_slot_values:
      val = row_vals[slot_idx]
      if hasattr(val, 'to_api_repr'):
        param_type = getattr(val, 'type_', None)
        if isinstance(param_type, str):
          fallback_types[slot_idx] = param_type
          break
      elif isinstance(val, (list, tuple)):
        non_null = next((item for item in val if item is not None), None)
        if non_null is not None:
          fallback_types[slot_idx] = _infer_bq_scalar_type(non_null)
          break
      elif val is not None:
        fallback_types[slot_idx] = _infer_bq_scalar_type(val)
        break
  return fallback_types


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
    validate_bigquery_identifier(table_name, 'table_name', TABLE_NAME_PATTERN)
  if column_names:
    for col in column_names:
      validate_bigquery_identifier(col, 'column_name', COLUMN_NAME_PATTERN)


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
  To override this, use the `column_names` parameter to specify a list of column
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
    Example Usage::

      handler = BigQueryEnrichmentHandler(
          project=project_name,
          row_restriction_template="id = ?",
          table_name='project.dataset.table',
          fields=fields,
          min_batch_size=2,
          max_batch_size=100)

    Args:
      project: Google Cloud project ID for the BigQuery table.
      table_name (str): Fully qualified BigQuery table name in the format
        `project.dataset.table`.
      row_restriction_template (str): A parameterized template string for the
        `WHERE` clause in the BigQuery query. Supports positional `?`
        placeholders, `{}` / `{0}` / `{field}` placeholders, and `@field`
        placeholders. Legacy quoted placeholders such as `'{}'` automatically
        strip surrounding quotes and bind values via `bigquery.QueryJobConfig`
        query parameters.
      fields: (Optional[list[str]]) List of field names present in the input
        `beam.Row`. These are used to populate query parameters in the `WHERE`
        clause and join response rows (if `condition_value_fn` is not
        provided).
      column_names: (Optional[list[str]]) Names of columns to select from the
        BigQuery table. If not provided, all columns (`*`) are selected.
      condition_value_fn: (Optional[Callable[[beam.Row], list[Any]]]) A function
        that takes a `beam.Row` and returns a list of values to bind to the
        query parameters in the `WHERE` clause.
      query_fn: (Optional[Callable[[beam.Row], Union[str, tuple[str,
        Sequence[Any]]]]]) A function that takes a `beam.Row` and returns
        either a `(query_sql, query_parameters)` tuple (recommended) or a raw
        BigQuery SQL string. Warning: returning a raw `str` bypasses automatic
        query parameterization; callers returning raw SQL strings must never
        interpolate untrusted row values directly into SQL text.
      min_batch_size (int): Minimum number of rows to batch together when
        querying BigQuery. Defaults to 1 if `query_fn` is not specified.
      max_batch_size (int): Maximum number of rows to batch together.
        Defaults to 10,000 if `query_fn` is not specified.
      max_batch_duration_secs (float): Maximum amount of time in seconds to
        buffer a batch before emitting it. If not provided, batching duration
        is determined by `BatchElements` defaults.
      throw_exception_on_empty_results (bool): Whether to raise `ValueError`
        when BigQuery returns no matching row for an input element.
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
    if not query_fn and self.fields and not condition_value_fn:
      for spec in self._placeholder_specs:
        if isinstance(spec, int) and spec >= len(self.fields):
          raise ValueError(
              f"Placeholder index {spec} out of range for "
              f"{len(self.fields)} field(s) in `fields`.")
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

  def _resolve_row_slot_values(
      self,
      current_values: Sequence[Any],
      request_dict: Optional[Mapping[str, Any]] = None,
  ) -> list[Any]:
    if not self._placeholder_specs:
      return []
    resolved: list[Any] = []
    for spec in self._placeholder_specs:
      if isinstance(spec, int):
        if spec >= len(current_values):
          raise ValueError(
              f"Placeholder index {spec} out of range for "
              f"{len(current_values)} condition value(s).")
        val = current_values[spec]
      else:
        if request_dict is not None and spec in request_dict:
          val = request_dict[spec]
        elif spec in self.fields:
          val = current_values[self.fields.index(spec)]
        else:
          raise KeyError(
              f"Placeholder field {spec!r} not found in input `beam.Row`.")
      resolved.append(val)
    return resolved

  def _build_row_parameters(
      self,
      current_values: Sequence[Any],
      request_dict: Optional[Mapping[str, Any]] = None,
      fallback_types: Optional[Sequence[Optional[str]]] = None,
  ) -> list[Any]:
    slot_values = self._resolve_row_slot_values(current_values, request_dict)
    return [
        to_query_parameter(
            val,
            fallback_type=(
                fallback_types[idx] if fallback_types is not None else None),
        ) for idx, val in enumerate(slot_values)
    ]

  def _build_batch_query(self, chunk_size: int) -> str:
    if chunk_size <= 1:
      return self.query_template
    batched_condition_template = ' or '.join(
        [f'({self._normalized_row_restriction_template})'] * chunk_size)
    return (
        "SELECT %s FROM %s WHERE %s" %
        (self.select_fields, self.table_name, batched_condition_template))

  def _execute_query(
      self, query: str, query_parameters: Optional[Sequence[Any]] = None):
    try:
      if query_parameters:
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
      return tuple(_make_hashable(v) for v in self.condition_value_fn(row))
    if self.fields:
      row_dict = row._asdict()
      return tuple(_make_hashable(row_dict[field]) for field in self.fields)
    raise ValueError("Either fields or condition_value_fn must be specified")

  def __call__(self, request: Union[beam.Row, list[beam.Row]], *args, **kwargs):
    if isinstance(request, list):
      if not request:
        return []
      responses = []
      requests_map: dict[Any, list[beam.Row]] = defaultdict(list)
      all_row_slot_values: list[list[Any]] = []

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
        all_row_slot_values.append(
            self._resolve_row_slot_values(current_values, request_dict))
        requests_map[self.create_row_key(req)].append(req)

      num_slots = len(self._placeholder_specs)
      slot_fallback_types = _infer_slot_fallback_types(
          all_row_slot_values, num_slots)
      max_rows_per_query = (
          max(1, MAX_BQ_QUERY_PARAMETERS //
              num_slots) if num_slots > 0 else len(request))

      responses_dict: list[dict[str, Any]] = []
      last_query = self.query_template
      for start_idx in range(0, len(request), max_rows_per_query):
        chunk_slots = all_row_slot_values[start_idx:start_idx +
                                          max_rows_per_query]
        chunk_params: list[Any] = []
        for slot_values in chunk_slots:
          for slot_idx, val in enumerate(slot_values):
            chunk_params.append(
                to_query_parameter(
                    val, fallback_type=slot_fallback_types[slot_idx]))
        last_query = self._build_batch_query(len(chunk_slots))
        chunk_resp = self._execute_query(last_query, chunk_params)
        if chunk_resp:
          responses_dict.extend(chunk_resp)

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
          raise ValueError(f"no matching row found for query: {last_query}")
        else:
          _LOGGER.warning('no matching row found for query: %s', last_query)
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
          single_query_parameters = [to_query_parameter(v) for v in raw_params]
        else:
          query = query_result
      else:
        try:
          values = (
              self.condition_value_fn(request) if self.condition_value_fn else
              [request_dict[field] for field in self.fields])
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `fields` are the "
              "keys in the input `beam.Row`." + str(e))
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
