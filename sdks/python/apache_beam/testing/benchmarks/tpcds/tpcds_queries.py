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

"""TPC-DS SQL query loader and table identifier extraction."""

import os
import re
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from apache_beam.testing.benchmarks.tpcds.tpcds_schemas import TPCDS_BUILD_DIR
from apache_beam.testing.benchmarks.tpcds.tpcds_schemas import get_all_table_names

TPCDS_QUERIES_DIR = os.path.join(TPCDS_BUILD_DIR, "queries")

# The 13 canonical multi-table TPC-DS star/snowflake aggregate queries.
CANONICAL_13_QUERIES: List[str] = [
    "3", "7", "13", "15", "19", "26", "42", "43", "48", "52", "55", "91", "96"
]

# TPC-DS queries supported by Beam Calcite SQL (returning > 0 rows).
# Together with KNOWN_FAILING_QUERIES, this partitions all 103 TPC-DS queries.
# yapf: disable
SUPPORTED_QUERIES: List[str] = [
    "1", "3", "4", "7", "9", "10", "11", "12", "15", "17",
    "19", "20", "21", "26", "31", "32", "33", "34", "35", "38",
    "40", "42", "43", "45", "46", "49", "50", "52", "53", "55",
    "56", "59", "60", "61", "63", "65", "66", "68", "69", "71",
    "73", "74", "75", "76", "79", "82", "84", "87", "88", "89",
    "90", "92", "93", "96", "97", "98", "99",
]
# yapf: enable

# Known failing TPC-DS queries on Beam Calcite SQL with their expected error substring.
# Used by `--check_regression` and `:sdks:python:apache_beam:testing:benchmarks:tpcds:checkAllQueries`
# to verify that all SUPPORTED_QUERIES succeed on PrismRunner and that the remaining 48 known
# failing queries fail with their expected diagnostic without regressing.
# pylint: disable=line-too-long
KNOWN_FAILING_QUERIES: Dict[str, str] = {
    "query2": "UnsupportedOperationException: Only support column reference or struct field access in conjunction clause",
    "query5": "Error while applying rule BeamBasicAggregationRule",
    "query6": "VolcanoPlanner timeout (correlated subquery + SINGLE_VALUE join permutation search)",
    "query8": "UnsupportedOperationException: Only support column reference or struct field access in conjunction clause",
    "query13": "UnsupportedOperationException: Operator OR is not supported in join condition",
    "query14a": "Error while applying rule BeamBasicAggregationRule",
    "query14b": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query16": "UnsupportedOperationException: Does not support COUNT DISTINCT",
    "query18": "Error while applying rule BeamBasicAggregationRule",
    "query22": "Error while applying rule BeamBasicAggregationRule",
    "query23a": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query23b": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query24a": "VolcanoPlanner timeout (>12-table join permutation search)",
    "query24b": "VolcanoPlanner timeout (>12-table join permutation search)",
    "query25": "Returned 0 rows",
    "query27": "Error while applying rule BeamBasicAggregationRule",
    "query28": "ParseException: Unable to parse query",
    "query29": "Returned 0 rows",
    "query30": "UnsupportedOperationException: Cross join with a residual (non-equi) condition is not supported yet.",
    "query36": "Error while applying rule BeamBasicAggregationRule",
    "query37": "Returned 0 rows",
    "query39a": "Returned 0 rows",
    "query39b": "Returned 0 rows",
    "query41": "ParseException: Unable to parse query",
    "query44": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query47": "IllegalArgumentException: DECIMAL NOT NULL is not nullable",
    "query48": "UnsupportedOperationException: Operator OR is not supported in join condition",
    "query51": "IllegalArgumentException: DECIMAL NOT NULL is not nullable",
    "query54": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query57": "Returned 0 rows",
    "query58": "UnsupportedOperationException: Aggregator [SINGLE_VALUE] is not supported",
    "query62": "Error while applying rule JoinPushExpressionsRule",
    "query64": "VolcanoPlanner timeout (36-table join permutation search)",
    "query67": "Error while applying rule BeamBasicAggregationRule",
    "query70": "Error while applying rule BeamBasicAggregationRule",
    "query72": "ParseException: Unable to parse query",
    "query77": "Error while applying rule BeamBasicAggregationRule",
    "query78": "CalcFn failed to evaluate",
    "query80": "Error while applying rule BeamBasicAggregationRule",
    "query81": "UnsupportedOperationException: Cross join with a residual (non-equi) condition is not supported yet.",
    "query83": "Error decoding input stream with coder",
    "query85": "Returned 0 rows",
    "query86": "Error while applying rule BeamBasicAggregationRule",
    "query91": "VolcanoPlanner timeout (7-table join permutation search)",
    "query94": "Error while applying rule JoinPushExpressionsRule",
    "query95": "Error while applying rule JoinPushExpressionsRule",
}
# pylint: enable=line-too-long

# Subset of KNOWN_FAILING_QUERIES where Calcite's VolcanoPlanner explores exponential join
# permutations (7 to 36 table joins) before failing with CannotPlanException.
PLANNER_TIMEOUT_QUERIES: Set[str] = {
    "query6", "query24a", "query24b", "query64", "query91"
}


def _require_queries_dir(queries_dir: Optional[str] = None) -> str:
  target_dir = queries_dir or TPCDS_QUERIES_DIR
  if not os.path.isdir(target_dir):
    raise RuntimeError(
        f"TPC-DS queries directory not found at {target_dir}. "
        "Running TPC-DS as a standalone apache-beam module is not supported; "
        "run via Gradle (:sdks:python:apache_beam:testing:benchmarks:tpcds:copyTpcdsResources) "
        "to populate tpcds/build.")
  return target_dir


def get_all_query_names() -> List[str]:
  """Returns all 103 TPC-DS query names (query1 .. query99 including a/b variants)."""
  names: List[str] = []
  split_queries = {14, 23, 24, 39}
  for i in range(1, 100):
    if i in split_queries:
      names.append(f"query{i}a")
      names.append(f"query{i}b")
    else:
      names.append(f"query{i}")
  return names


def normalize_query_name(q: str) -> str:
  """Normalizes '3', '03', 'q03', 'query3' -> 'query3'."""
  cleaned = q.strip().lower()
  m = re.match(r"^(?:query|q)?0*(\d+[ab]?)$", cleaned)
  if m:
    return f"query{m.group(1)}"
  return cleaned


def parse_query_selection(queries_arg: str) -> List[str]:
  """Parses a --queries argument ('canonical13', 'supported', 'all', or comma-separated list)."""
  cleaned = queries_arg.strip().lower()
  if cleaned in ("canonical13", "canonical"):
    return [normalize_query_name(q) for q in CANONICAL_13_QUERIES]
  if cleaned == "supported":
    return [normalize_query_name(q) for q in SUPPORTED_QUERIES]
  if cleaned == "all":
    return get_all_query_names()
  all_valid = set(get_all_query_names())
  result = []
  for part in re.split(r"[\s,]+", queries_arg.strip()):
    if not part:
      continue
    qname = normalize_query_name(part)
    if qname not in all_valid:
      raise ValueError(f"Unsupported TPC-DS query name: {part!r} ({qname})")
    result.append(qname)
  return result


def read_query(query_name: str, queries_dir: Optional[str] = None) -> str:
  """Reads a TPC-DS SQL query by name (e.g. 'query3' or '3') from tpcds/build/queries."""
  qname = normalize_query_name(query_name)
  target_dir = _require_queries_dir(queries_dir)
  candidate = os.path.join(target_dir, f"{qname}.sql")
  if not os.path.isfile(candidate):
    raise RuntimeError(
        f"Could not find SQL file for {qname} at {candidate}. "
        "Run via Gradle (:sdks:python:apache_beam:testing:benchmarks:tpcds:copyTpcdsResources)."
    )
  with open(candidate, "r", encoding="utf-8") as f:
    return _strip_sql_comments(f.read())


def _strip_sql_comments(sql: str) -> str:
  lines = []
  for line in sql.splitlines():
    if line.lstrip().startswith("--"):
      continue
    lines.append(line)
  return "\n".join(lines).strip()


def get_query_tables(query_sql: str) -> List[str]:
  """Extracts the TPC-DS table names referenced in the given SQL query."""
  tokens: Set[str] = set(
      re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", query_sql.lower()))
  return [t for t in get_all_table_names() if t.lower() in tokens]
