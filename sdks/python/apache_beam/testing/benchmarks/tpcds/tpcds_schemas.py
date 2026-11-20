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

"""TPC-DS table schemas and row converters for Beam Python SQL."""

import decimal
import json
import os
import typing
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from apache_beam import coders

TPCDS_BUILD_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "build")
TPCDS_SCHEMAS_DIR = os.path.join(TPCDS_BUILD_DIR, "schemas")

_SCHEMA_CACHE: Dict[str, List[Tuple[str, str]]] = {}
_ROW_CLASS_CACHE: Dict[str, Type[Tuple[Any, ...]]] = {}


def _require_schemas_dir() -> str:
  if not os.path.isdir(TPCDS_SCHEMAS_DIR):
    raise RuntimeError(
        f"TPC-DS schema directory not found at {TPCDS_SCHEMAS_DIR}. "
        "Running TPC-DS as a standalone apache-beam module is not supported; "
        "run via Gradle (:sdks:python:apache_beam:testing:benchmarks:tpcds:copyTpcdsResources) "
        "to populate tpcds/build.")
  return TPCDS_SCHEMAS_DIR


def get_raw_table_schema(table_name: str) -> List[Tuple[str, str]]:
  """Returns the list of (column_name, raw_type) for a TPC-DS table from tpcds/build/schemas."""
  if table_name in _SCHEMA_CACHE:
    return list(_SCHEMA_CACHE[table_name])

  schemas_dir = _require_schemas_dir()
  json_path = os.path.join(schemas_dir, f"{table_name}.json")
  if not os.path.isfile(json_path):
    raise RuntimeError(
        f"TPC-DS schema file for table {table_name!r} not found at {json_path}. "
        "Run via Gradle (:sdks:python:apache_beam:testing:benchmarks:tpcds:copyTpcdsResources)."
    )
  with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)
  cols = [(col["name"], col["type"].lower()) for col in data["schema"]]
  _SCHEMA_CACHE[table_name] = cols
  return list(cols)


def get_all_table_names() -> List[str]:
  """Returns sorted list of all TPC-DS table names from tpcds/build/schemas."""
  schemas_dir = _require_schemas_dir()
  names = sorted(
      os.path.splitext(fname)[0] for fname in os.listdir(schemas_dir)
      if fname.endswith(".json"))
  if not names:
    raise RuntimeError(f"No TPC-DS schema JSON files found in {schemas_dir}.")
  return names


def _python_field_type(raw_type: str) -> Any:
  """Maps a raw TPC-DS JSON schema type to a nullable Python type hint for RowCoder."""
  t = raw_type.lower()
  if t in ("identifier", "integer"):
    return Optional[int]
  elif "decimal" in t:
    return Optional[decimal.Decimal]
  else:
    return Optional[str]


def get_table_row_class(table_name: str) -> Type[Tuple[Any, ...]]:
  """Returns a NamedTuple class registered with RowCoder for the given table."""
  if table_name in _ROW_CLASS_CACHE:
    return _ROW_CLASS_CACHE[table_name]

  raw_cols = get_raw_table_schema(table_name)
  fields = [(col_name, _python_field_type(col_type))
            for col_name, col_type in raw_cols]
  type_name = f"Tpcds_{table_name}"
  row_cls = typing.NamedTuple(type_name, fields)
  coders.registry.register_coder(row_cls, coders.RowCoder)
  _ROW_CLASS_CACHE[table_name] = row_cls
  return row_cls


def coerce_value(val: Any, raw_type: str) -> Any:
  """Coerces a Python or string value to match the target column type."""
  if val is None or val == "":
    return None
  t = raw_type.lower()
  if t in ("identifier", "integer"):
    return int(val)
  elif "decimal" in t:
    if isinstance(val, decimal.Decimal):
      return val
    if isinstance(val, float):
      return decimal.Decimal(f"{val:.2f}")
    return decimal.Decimal(str(val))
  else:
    return str(val)


def parse_csv_line(table_name: str,
                   line: str,
                   delimiter: str = "|") -> Optional[Any]:
  """Parses a TPC-DS .dat pipe-delimited line into a typed NamedTuple row."""
  stripped = line.rstrip("\r\n")
  if not stripped:
    return None
  raw_cols = get_raw_table_schema(table_name)
  row_cls = get_table_row_class(table_name)
  parts = stripped.split(delimiter)
  # TPC-DS .dat files have a trailing '|' delimiter
  if len(parts) == len(raw_cols) + 1 and parts[-1] == "":
    parts = parts[:-1]
  args = []
  for idx, (_, col_type) in enumerate(raw_cols):
    raw_val = parts[idx] if idx < len(parts) else ""
    args.append(coerce_value(raw_val, col_type))
  return row_cls(*args)
