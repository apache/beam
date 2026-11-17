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
import csv
import io
import json
import logging
import math
import os
from typing import Any
from typing import Optional

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
from apache_beam.utils import shared

try:
  import pyarrow as pa
  import pyarrow.parquet as pq
except ImportError:
  pa = None  # type: ignore[assignment]
  pq = None  # type: ignore[assignment]

__all__ = [
    'FileEnrichmentHandler',
]

_LOGGER = logging.getLogger(__name__)

_FORMAT_ALIASES = {
    'parquet': 'parquet',
    'pq': 'parquet',
    'csv': 'csv',
    'tsv': 'tsv',
    'json': 'json',
    'jsonl': 'jsonl',
    'ndjson': 'jsonl',
}


class _LookupTable:
  """Weak-referenceable container for in-memory reference records and schema."""
  def __init__(
      self,
      records: dict[Any, dict[str, Any]],
      str_records: dict[str, dict[str, Any]],
      schema: list[tuple[str, Any]],
  ):
    self.records = records
    self.str_records = str_records
    self.schema = schema

  def get(self, key: Any) -> Optional[dict[str, Any]]:
    if key is None:
      return self.records.get(None)
    if key in self.records:
      return self.records[key]
    str_key = str(key)
    if str_key in self.str_records:
      return self.str_records[str_key]
    return None


def _arrow_type_to_python_type(arrow_type: Any) -> Any:
  if pa is None:
    return Any
  if pa.types.is_boolean(arrow_type):
    return bool
  if pa.types.is_integer(arrow_type):
    return int
  if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
    return float
  if (pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type) or
      pa.types.is_unicode(arrow_type)):
    return str
  if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
    return bytes
  if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
    elem_type = _arrow_type_to_python_type(arrow_type.value_type)
    return list[elem_type]  # type: ignore[valid-type]
  return Any


def _is_int_string(val: str) -> bool:
  s = val.strip()
  if not s:
    return False
  unsigned = s[1:] if s[0] in ('+', '-') else s
  if not unsigned.isdigit():
    return False
  if len(unsigned) > 1 and unsigned.startswith('0'):
    return False
  return True


def _is_float_string(val: str) -> bool:
  s = val.strip()
  if not s:
    return False
  try:
    parsed = float(s)
    return not (math.isnan(parsed) or math.isinf(parsed))
  except ValueError:
    return False


def _infer_and_convert_csv_column(
    raw_values: list[Optional[str]],
    parse_types: bool = True) -> tuple[list[Any], Any]:
  has_null = any(v is None or v == '' for v in raw_values)
  non_empty = [v for v in raw_values if v is not None and v != '']
  if not non_empty or not parse_types:
    converted = [None if (v is None or v == '') else v for v in raw_values]
    base_type: Any = str
    return converted, (Optional[base_type] if has_null else base_type)

  if all(v.strip().lower() in ('true', 'false') for v in non_empty):
    converted = [
        None if (v is None or v == '') else (v.strip().lower() == 'true')
        for v in raw_values
    ]
    base_type = bool
  elif all(_is_int_string(v) for v in non_empty):
    converted = [
        None if (v is None or v == '') else int(v.strip()) for v in raw_values
    ]
    base_type = int
  elif all(_is_float_string(v) for v in non_empty):
    converted = [
        None if (v is None or v == '') else float(v.strip()) for v in raw_values
    ]
    base_type = float
  else:
    converted = [None if (v is None or v == '') else v for v in raw_values]
    base_type = str

  return converted, (Optional[base_type] if has_null else base_type)


def _infer_python_column_type(values: list[Any]) -> Any:
  has_null = any(v is None for v in values)
  non_null = [v for v in values if v is not None]
  if not non_null:
    return Optional[Any]

  types_seen = {type(v) for v in non_null}
  if types_seen == {bool}:
    base_type: Any = bool
  elif types_seen == {int}:
    base_type = int
  elif types_seen <= {int, float}:
    base_type = float
  elif types_seen == {str}:
    base_type = str
  elif types_seen == {bytes}:
    base_type = bytes
  else:
    base_type = Any

  return Optional[base_type] if has_null else base_type


class FileEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """Enrichment handler for file-backed reference tables (Parquet, CSV, JSON/JSONL).

  Loads a reference file via :class:`apache_beam.io.filesystems.FileSystems`
  in worker ``__enter__`` and shares the in-memory lookup dictionary across
  worker threads via :class:`apache_beam.utils.shared.Shared`.

  Args:
    path (str): Path or file pattern to the reference file (e.g. `.parquet`,
      `.csv`, `.json`, or `.jsonl` on local disk, GCS, or S3).
    join_key (str): Key column name in the reference file (and in the input
      `beam.Row` unless `row_key` is specified separately).
    row_key (Optional[str]): Field name in the input `beam.Row` to use for
      lookup. Defaults to `join_key`.
    select_fields (Optional[list[str]]): List of column names to select from
      the reference file. If `None`, all columns other than `join_key` are
      returned.
    file_format (Optional[str]): Explicit file format (`"parquet"`, `"csv"`,
      `"json"`, or `"jsonl"`). If omitted, inferred from `path`.
    format (Optional[str]): Alias for `file_format`.
    delimiter (str): Field delimiter for CSV files. Defaults to `","`.
    encoding (str): Text encoding for CSV/JSON/JSONL files. Defaults to
      `"utf-8"`.
    parse_csv_types (bool): Whether to infer primitive types (`int`, `float`,
      `bool`, `str`) for CSV columns. Defaults to `True`.
    exception_level (ExceptionLevel): Controls behavior when a key is not
      found in the reference file. Defaults to `ExceptionLevel.QUIET`.
  """
  def __init__(
      self,
      path: str,
      join_key: str = "",
      *,
      row_key: Optional[str] = None,
      select_fields: Optional[list[str]] = None,
      file_format: Optional[str] = None,
      format: Optional[str] = None,  # pylint: disable=redefined-builtin
      delimiter: str = ',',
      encoding: str = 'utf-8',
      parse_csv_types: bool = True,
      exception_level: ExceptionLevel = ExceptionLevel.QUIET,
  ):
    if not path:
      raise ValueError("`path` must be provided for FileEnrichmentHandler.")
    if not join_key and not row_key:
      raise ValueError(
          "Please specify `join_key` (or `row_key`) to identify the lookup "
          "key column.")
    self._path = path
    self._join_key = join_key or (row_key or "")
    self._row_key = row_key or self._join_key
    self._select_fields = list(
        select_fields) if select_fields is not None else None
    raw_format = file_format or format or self._infer_format(path)
    normalized_format = _FORMAT_ALIASES.get(raw_format.lower())
    if not normalized_format:
      raise ValueError(
          f"Unsupported file format '{raw_format}'. Supported formats are: "
          "parquet, csv, json, jsonl.")
    self._format = normalized_format
    self._delimiter = '\t' if (
        self._format == 'tsv' and delimiter == ',') else delimiter
    self._encoding = encoding
    self._parse_csv_types = parse_csv_types
    self._exception_level = exception_level
    self._shared_handle = shared.Shared()
    self._table: Optional[_LookupTable] = None
    self._cached_schema: Optional[list[tuple[str, Any]]] = None

  @staticmethod
  def _infer_format(path: str) -> str:
    clean_path = path.rstrip('*')
    _, ext = os.path.splitext(clean_path)
    ext = ext.lstrip('.').lower()
    if ext in ('gz', 'bz2', 'zst', 'snappy'):
      _, ext = os.path.splitext(clean_path[:-len(ext) - 1])
      ext = ext.lstrip('.').lower()
    if not ext:
      raise ValueError(
          f"Could not infer file format from path '{path}'. Please specify "
          "`file_format` explicitly.")
    return ext

  def _resolve_paths(self) -> list[str]:
    try:
      match_results = FileSystems.match([self._path])
      if match_results and match_results[0].metadata_list:
        return [m.path for m in match_results[0].metadata_list]
    except Exception:
      pass
    if FileSystems.exists(self._path):
      return [self._path]
    raise FileNotFoundError(f"Reference file not found at path: {self._path}")

  def _determine_target_columns(self, all_columns: list[str]) -> list[str]:
    if self._join_key not in all_columns:
      raise KeyError(
          f"join_key '{self._join_key}' not found in reference file columns: "
          f"{all_columns}")
    if self._select_fields is not None:
      missing = [c for c in self._select_fields if c not in all_columns]
      if missing:
        raise KeyError(
            f"select_fields {missing} not found in reference file columns: "
            f"{all_columns}")
      return list(self._select_fields)
    return [c for c in all_columns if c != self._join_key]

  def _load_parquet(self, file_paths: list[str]) -> _LookupTable:
    if pa is None or pq is None:
      raise ImportError(
          "pyarrow is required to read Parquet files in FileEnrichmentHandler.")
    tables = []
    for file_path in file_paths:
      with FileSystems.open(file_path, 'rb') as f:
        data = f.read()
      tables.append(pq.read_table(pa.BufferReader(data)))

    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    target_cols = self._determine_target_columns(combined.column_names)

    rows = combined.to_pylist()
    records: dict[Any, dict[str, Any]] = {}
    str_records: dict[str, dict[str, Any]] = {}
    col_values: dict[str, list[Any]] = {c: [] for c in target_cols}

    for row in rows:
      key = row.get(self._join_key)
      projected = {c: row.get(c) for c in target_cols}
      for c in target_cols:
        col_values[c].append(projected[c])
      records[key] = projected
      if key is not None:
        str_records[str(key)] = projected

    schema: list[tuple[str, Any]] = []
    arrow_schema = combined.schema
    for c in target_cols:
      field = arrow_schema.field(c)
      py_type = _arrow_type_to_python_type(field.type)
      has_null = any(v is None for v in col_values[c])
      if has_null and py_type is not Any:
        py_type = Optional[py_type]
      schema.append((c, py_type))

    return _LookupTable(records, str_records, schema)

  def _load_csv(self, file_paths: list[str]) -> _LookupTable:
    raw_rows: list[dict[str, Optional[str]]] = []
    fieldnames: list[str] = []
    for file_path in file_paths:
      with FileSystems.open(file_path, 'rb') as f:
        text = f.read().decode(self._encoding)
      reader = csv.DictReader(io.StringIO(text), delimiter=self._delimiter)
      if reader.fieldnames and not fieldnames:
        fieldnames = list(reader.fieldnames)
      for row in reader:
        raw_rows.append(row)

    target_cols = self._determine_target_columns(fieldnames)
    key_raw = [r.get(self._join_key) for r in raw_rows]
    typed_keys, _ = _infer_and_convert_csv_column(
        key_raw, parse_types=self._parse_csv_types)

    typed_cols: dict[str, list[Any]] = {}
    schema: list[tuple[str, Any]] = []
    for c in target_cols:
      col_raw = [r.get(c) for r in raw_rows]
      converted, col_type = _infer_and_convert_csv_column(
          col_raw, parse_types=self._parse_csv_types)
      typed_cols[c] = converted
      schema.append((c, col_type))

    records: dict[Any, dict[str, Any]] = {}
    str_records: dict[str, dict[str, Any]] = {}
    for idx, key in enumerate(typed_keys):
      projected = {c: typed_cols[c][idx] for c in target_cols}
      records[key] = projected
      if key is not None:
        str_records[str(key)] = projected
      raw_k = key_raw[idx]
      if raw_k is not None:
        str_records[raw_k] = projected

    return _LookupTable(records, str_records, schema)

  def _load_json(self, file_paths: list[str]) -> _LookupTable:
    raw_rows: list[dict[str, Any]] = []
    all_columns: list[str] = []
    seen_columns: set[str] = set()

    for file_path in file_paths:
      with FileSystems.open(file_path, 'rb') as f:
        text = f.read().decode(self._encoding).strip()
      if not text:
        continue
      if self._format == 'json' and text.startswith('['):
        parsed = json.loads(text)
        if not isinstance(parsed, list):
          raise ValueError(
              f"Expected JSON array of objects in '{file_path}', got "
              f"{type(parsed).__name__}.")
        file_rows = parsed
      else:
        file_rows = [
            json.loads(line) for line in text.splitlines() if line.strip()
        ]
      for row in file_rows:
        if not isinstance(row, dict):
          raise ValueError(
              f"Expected JSON object rows in '{file_path}', got "
              f"{type(row).__name__}.")
        for k in row.keys():
          if k not in seen_columns:
            seen_columns.add(k)
            all_columns.append(k)
        raw_rows.append(row)

    target_cols = self._determine_target_columns(all_columns)
    records: dict[Any, dict[str, Any]] = {}
    str_records: dict[str, dict[str, Any]] = {}
    col_values: dict[str, list[Any]] = {c: [] for c in target_cols}

    for row in raw_rows:
      key = row.get(self._join_key)
      projected = {c: row.get(c) for c in target_cols}
      for c in target_cols:
        col_values[c].append(projected[c])
      records[key] = projected
      if key is not None:
        str_records[str(key)] = projected

    schema: list[tuple[str,
                       Any]] = [(c, _infer_python_column_type(col_values[c]))
                                for c in target_cols]
    return _LookupTable(records, str_records, schema)

  def _load_lookup_table(self) -> _LookupTable:
    file_paths = self._resolve_paths()
    if self._format == 'parquet':
      return self._load_parquet(file_paths)
    if self._format in ('csv', 'tsv'):
      return self._load_csv(file_paths)
    if self._format in ('json', 'jsonl'):
      return self._load_json(file_paths)
    raise ValueError(f"Unsupported file format: {self._format}")

  def get_output_schema(self) -> list[tuple[str, Any]]:
    """Returns the static `(field_name, field_type)` schema of enriched fields."""
    if self._cached_schema is not None:
      return list(self._cached_schema)
    try:
      table = self._shared_handle.acquire(self._load_lookup_table)
      self._cached_schema = list(table.schema)
      return list(self._cached_schema)
    except Exception:
      if self._select_fields is not None:
        return [(f, Any) for f in self._select_fields]
      return []

  def __enter__(self):
    self._table = self._shared_handle.acquire(self._load_lookup_table)
    if self._cached_schema is None:
      self._cached_schema = list(self._table.schema)
    return self

  def __call__(self, request: beam.Row, *args, **kwargs):
    if self._table is None:
      self.__enter__()
    assert self._table is not None

    request_dict = request._asdict()
    if self._row_key not in request_dict:
      raise KeyError(
          f"join_key '{self._row_key}' not found in input PCollection row: "
          f"{request}")

    key_val = request_dict[self._row_key]
    matched = self._table.get(key_val)
    if matched is None:
      if self._exception_level == ExceptionLevel.WARN:
        _LOGGER.warning(
            'no matching row found for %s: %s in %s',
            self._row_key,
            key_val,
            self._path)
      elif self._exception_level == ExceptionLevel.RAISE:
        raise ValueError(
            f"no matching row found for {self._row_key}: {key_val} in "
            f"{self._path}")
      return request, beam.Row()

    return request, beam.Row(**matched)

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._table = None

  def __getstate__(self):
    state = dict(self.__dict__)
    state['_table'] = None
    return state

  def get_cache_key(self, request: beam.Row) -> str:
    return "%s: %s" % (self._row_key, request._asdict()[self._row_key])
