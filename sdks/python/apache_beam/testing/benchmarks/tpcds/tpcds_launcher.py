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

"""TPC-DS launcher for Apache Beam Python using cross-language SqlTransform and PrismRunner.

Example usage via Gradle:
  ./gradlew :sdks:python:apache_beam:testing:benchmarks:tpcds:run \\
      -Ptpcds.args="--queries=supported --data_size=10MB"
"""

import argparse
import contextlib
import dataclasses
import datetime
import glob
import json
import logging
import os
import tempfile
import time
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api import schema_pb2
from apache_beam.testing.benchmarks.tpcds.tpcds_queries import KNOWN_FAILING_QUERIES
from apache_beam.testing.benchmarks.tpcds.tpcds_queries import PLANNER_TIMEOUT_QUERIES
from apache_beam.testing.benchmarks.tpcds.tpcds_queries import get_query_tables
from apache_beam.testing.benchmarks.tpcds.tpcds_queries import parse_query_selection
from apache_beam.testing.benchmarks.tpcds.tpcds_queries import read_query
from apache_beam.testing.benchmarks.tpcds.tpcds_schemas import get_all_table_names
from apache_beam.testing.benchmarks.tpcds.tpcds_schemas import get_table_row_class
from apache_beam.testing.benchmarks.tpcds.tpcds_schemas import parse_csv_line
from apache_beam.transforms.sql import SqlTransform
from apache_beam.typehints import schemas as beam_schemas
from apache_beam.utils import subprocess_server

_LOGGER = logging.getLogger(__name__)

DEFAULT_DATA_DIRECTORY = "gs://beam-tpcds/datasets/text"
SUPPORTED_DATA_SIZES: Set[str] = {
    "10M",
    "10MB",
    "1G",
    "1GB",
    "10G",
    "10GB",
    "100G",
    "100GB",
}


def check_and_normalize_data_size(data_size: str) -> str:
  """Validates data_size against SUPPORTED_DATA_SIZES and normalizes '10M' -> '10MB', etc."""
  cleaned = data_size.strip().upper()
  if cleaned not in SUPPORTED_DATA_SIZES:
    raise ValueError(
        f"The data size {data_size!r} has not been supported. "
        f"Supported sizes: {sorted(SUPPORTED_DATA_SIZES)}")
  if cleaned.endswith(("M", "G")):
    return f"{cleaned}B"
  return cleaned


# TODO(https://github.com/apache/beam/issues/40698) remove this contextmanager once Python
# Row supports arbitrary field name
@contextlib.contextmanager
def _sql_schema_field_sanitizer() -> Iterator[None]:
  """Temporarily allows NamedTuple schema decoding for SQL aliases like EXPR$n or >120 days."""
  orig_fn = beam_schemas.SchemaTranslation.named_tuple_from_schema

  def _sanitized_named_tuple_from_schema(self, schema):
    if any(not f.name.isidentifier() for f in schema.fields):
      copied = schema_pb2.Schema()
      copied.CopyFrom(schema)
      for field in copied.fields:
        if not field.name.isidentifier():
          sanitized = "".join(
              ch if (ch.isalnum() or ch == "_") else "_" for ch in field.name)
          if not sanitized or sanitized[0].isdigit():
            sanitized = f"col_{sanitized}"
          field.name = sanitized
      schema = copied
    return orig_fn(self, schema)

  beam_schemas.SchemaTranslation.named_tuple_from_schema = (
      _sanitized_named_tuple_from_schema)
  try:
    yield
  finally:
    beam_schemas.SchemaTranslation.named_tuple_from_schema = orig_fn


@dataclasses.dataclass
class TpcdsQueryResult:
  query_name: str
  job_name: str
  data_size: str
  dialect: str
  is_successful: bool
  status: str
  start_time: str
  end_time: str
  elapsed_sec: float
  row_count: int = 0
  error_message: str = ""


def build_table_pcollections(
    pipeline: beam.Pipeline,
    table_names: Sequence[str],
    data_directory: str = DEFAULT_DATA_DIRECTORY,
    data_size: str = "10MB") -> Dict[str, beam.PCollection]:
  """Creates schema-typed input PCollections from TPC-DS .dat files."""
  normalized_size = check_and_normalize_data_size(data_size)
  tables: Dict[str, beam.PCollection] = {}
  base_dir = data_directory.rstrip("/")
  for table_name in table_names:
    row_cls = get_table_row_class(table_name)
    file_pattern = f"{base_dir}/{normalized_size}/{table_name}.dat"
    tables[table_name] = (
        pipeline
        | f"Read_{table_name}" >> beam.io.ReadFromText(file_pattern)
        | f"Parse_{table_name}" >> beam.FlatMap(
            lambda line, t=table_name:
            [r for r in [parse_csv_line(t, line)]
             if r is not None]).with_output_types(row_cls))
  return tables


def run_single_query(
    query_name: str,
    query_sql: str,
    pipeline_args: Sequence[str],
    data_directory: str = DEFAULT_DATA_DIRECTORY,
    data_size: str = "10MB",
    results_directory: Optional[str] = None,
    skip_planner_timeouts: bool = False) -> TpcdsQueryResult:
  """Executes a single TPC-DS query via SqlTransform."""
  normalized_size = check_and_normalize_data_size(data_size)
  start_dt = datetime.datetime.now(datetime.timezone.utc)
  t0 = time.monotonic()
  job_name = f"{query_name}_{int(time.time() * 1000)}"
  if skip_planner_timeouts and query_name in PLANNER_TIMEOUT_QUERIES:
    err_msg = KNOWN_FAILING_QUERIES[query_name]
    end_dt = datetime.datetime.now(datetime.timezone.utc)
    return TpcdsQueryResult(
        query_name=query_name,
        job_name=job_name,
        data_size=normalized_size,
        dialect="Calcite",
        is_successful=False,
        status="Failed (PLANNING)",
        start_time=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_time=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        elapsed_sec=0.0,
        error_message=err_msg,
    )
  table_names = get_query_tables(query_sql)

  opts_list = list(pipeline_args)
  if not any(a.startswith("--runner") for a in opts_list):
    opts_list.append("--runner=PrismRunner")

  options = PipelineOptions(opts_list)

  with tempfile.TemporaryDirectory(prefix=f"tpcds_{query_name}_") as tmp_dir:
    if results_directory:
      out_dir = os.path.join(results_directory, normalized_size)
      os.makedirs(out_dir, exist_ok=True)
      out_prefix = os.path.join(out_dir, query_name)
    else:
      out_prefix = os.path.join(tmp_dir, "out")
    stage = "PLANNING"
    try:
      with _sql_schema_field_sanitizer():
        with beam.Pipeline(options=options) as p:
          tables = build_table_pcollections(
              p,
              table_names=table_names,
              data_directory=data_directory,
              data_size=normalized_size,
          )
          sql_input = tables if len(tables) > 1 else next(iter(tables.values()))
          sql_out = sql_input | f"Sql_{query_name}" >> SqlTransform(query_sql)
          _ = (
              sql_out
              | f"Format_{query_name}" >> beam.Map(str)
              | f"Write_{query_name}" >> beam.io.WriteToText(
                  out_prefix, file_name_suffix=".txt", num_shards=1))
          stage = "RUNTIME"

      row_count = 0
      for fpath in sorted(glob.glob(f"{out_prefix}*.txt")):
        with open(fpath, "r", encoding="utf-8") as f:
          row_count += sum(1 for line in f if line.strip())

      elapsed = time.monotonic() - t0
      end_dt = datetime.datetime.now(datetime.timezone.utc)

      is_successful = row_count > 0
      if not is_successful:
        _LOGGER.warning(
            "Output records counter for job %r is %d", job_name, row_count)

      return TpcdsQueryResult(
          query_name=query_name,
          job_name=job_name,
          data_size=normalized_size,
          dialect="Calcite",
          is_successful=is_successful,
          status="Successful" if is_successful else "Failed (EMPTY_RESULT)",
          start_time=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
          end_time=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
          elapsed_sec=round(elapsed, 3),
          row_count=row_count,
          error_message="" if is_successful else "Returned 0 rows",
      )
    except Exception as exc:  # pylint: disable=broad-except
      elapsed = time.monotonic() - t0
      end_dt = datetime.datetime.now(datetime.timezone.utc)
      err_str = str(exc)
      first_line = err_str.splitlines()[0] if err_str else repr(exc)
      for line in err_str.splitlines():
        if "UnsupportedOperationException" in line or "CannotPlanException" in line:
          first_line = line.strip()
          break
      _LOGGER.error("%s failed at %s: %s", query_name, stage, first_line)
      return TpcdsQueryResult(
          query_name=query_name,
          job_name=job_name,
          data_size=normalized_size,
          dialect="Calcite",
          is_successful=False,
          status=f"Failed ({stage})",
          start_time=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
          end_time=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
          elapsed_sec=round(elapsed, 3),
          error_message=first_line,
      )


def format_summary_table(results: Sequence[TpcdsQueryResult]) -> str:
  """Formats the TPC-DS execution summary table."""
  headers = [
      "Query Name",
      "Data Size",
      "Dialect",
      "Status",
      "Rows",
      "Elapsed(s)",
      "Details / Notes",
  ]
  rows = []
  for r in results:
    note = f"Returned {r.row_count} row(s)" if r.is_successful else r.error_message
    if len(note) > 85:
      note = note[:82] + "..."
    rows.append([
        r.query_name,
        r.data_size,
        r.dialect,
        r.status,
        str(r.row_count) if r.is_successful else "-",
        f"{r.elapsed_sec:.2f}",
        note,
    ])

  widths = [len(h) for h in headers]
  for row in rows:
    for i, cell in enumerate(row):
      widths[i] = max(widths[i], len(cell))

  sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
  header_line = "|" + "|".join(
      f" {h.ljust(widths[i])} " for i, h in enumerate(headers)) + "|"
  lines = ["\nTPC-DS Query Execution Summary:", sep, header_line, sep]
  for row in rows:
    lines.append(
        "|" + "|".join(f" {c.ljust(widths[i])} "
                       for i, c in enumerate(row)) + "|")
  lines.append(sep)
  return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> List[TpcdsQueryResult]:
  parser = argparse.ArgumentParser(
      description="Run Apache Beam Python TPC-DS benchmark suite.")
  parser.add_argument(
      "--queries",
      default="supported",
      help=
      "Comma-separated query numbers (e.g. '3,7,96'), 'canonical13', 'supported', or 'all'.",
  )
  parser.add_argument(
      "--data_directory",
      default=DEFAULT_DATA_DIRECTORY,
      help=
      "Path or GCS URI containing TPC-DS text data files (<data_size>/<table>.dat).",
  )
  parser.add_argument(
      "--data_size",
      default="10MB",
      help=
      "Dataset scale factor ('10M', '10MB', '1G', '1GB', '10G', '10GB', '100G', '100GB').",
  )
  parser.add_argument(
      "--queries_directory",
      default=None,
      help="Optional path to directory containing query*.sql files.",
  )
  parser.add_argument(
      "--results_directory",
      default=None,
      help="Optional directory to write query results.",
  )
  parser.add_argument(
      "--summary_json",
      default=None,
      help="Optional file path to write JSON execution summary.",
  )
  parser.add_argument(
      "--check_regression",
      default=False,
      action=argparse.BooleanOptionalAction,
      help="Verify query outcomes against KNOWN_FAILING_QUERIES. Queries not in "
      "KNOWN_FAILING_QUERIES must succeed; queries in KNOWN_FAILING_QUERIES must "
      "fail with an error matching their expected error substring.",
  )

  known_args, pipeline_args = parser.parse_known_args(argv)
  # Fail early if tpcds/build/schemas has not been populated by Gradle.
  get_all_table_names()
  normalized_size = check_and_normalize_data_size(known_args.data_size)
  query_names = parse_query_selection(known_args.queries)

  all_results: List[TpcdsQueryResult] = []
  with subprocess_server.SubprocessServer.cache_subprocesses():
    for qname in query_names:
      query_sql = read_query(qname, known_args.queries_directory)
      res = run_single_query(
          query_name=qname,
          query_sql=query_sql,
          pipeline_args=pipeline_args,
          data_directory=known_args.data_directory,
          data_size=normalized_size,
          results_directory=known_args.results_directory,
          skip_planner_timeouts=known_args.check_regression,
      )
      all_results.append(res)

  print(format_summary_table(all_results))

  if known_args.summary_json:
    os.makedirs(
        os.path.dirname(os.path.abspath(known_args.summary_json)),
        exist_ok=True)
    with open(known_args.summary_json, "w", encoding="utf-8") as f:
      json.dump(
          [dataclasses.asdict(r) for r in all_results],
          f,
          indent=2,
          default=str,
      )

  if known_args.check_regression:
    verify_no_regressions(all_results)

  return all_results


def verify_no_regressions(
    results: Sequence[TpcdsQueryResult],
    known_failing: Optional[Dict[str, str]] = None) -> None:
  """Verifies that query results match SUPPORTED_QUERIES and KNOWN_FAILING_QUERIES.

  Raises RuntimeError if:
    - A query in SUPPORTED_QUERIES fails, or
    - A query in KNOWN_FAILING_QUERIES fails with an unexpected error message, or
    - A query is unclassified (in neither SUPPORTED_QUERIES nor KNOWN_FAILING_QUERIES).

  Queries in KNOWN_FAILING_QUERIES that succeed are tolerated with a warning.
  """
  expected_map = KNOWN_FAILING_QUERIES if known_failing is None else known_failing
  supported_set = set(parse_query_selection("supported"))
  regressions: List[str] = []
  unexpected_passes: List[str] = []
  expected_failures_matched = 0
  passed_queries = 0

  for r in results:
    expected_err = expected_map.get(r.query_name)
    if expected_err is not None:
      if r.is_successful:
        warn_msg = (
            f"[UNEXPECTED PASS] {r.query_name} is listed in "
            f"KNOWN_FAILING_QUERIES ({expected_err!r}) but succeeded! "
            f"Consider moving it to SUPPORTED_QUERIES if non-flaky.")
        _LOGGER.warning(warn_msg)
        unexpected_passes.append(warn_msg)
      elif expected_err not in r.error_message:
        regressions.append(
            f"[ERROR MISMATCH] {r.query_name} failed with "
            f"unexpected error: {r.error_message!r} (expected substring: {expected_err!r})"
        )
      else:
        expected_failures_matched += 1
    elif r.query_name in supported_set:
      if not r.is_successful:
        regressions.append(
            f"[REGRESSION] {r.query_name} is in SUPPORTED_QUERIES "
            f"but failed with: {r.error_message}")
      else:
        passed_queries += 1
    else:
      regressions.append(
          f"[UNCLASSIFIED] {r.query_name} is in neither SUPPORTED_QUERIES "
          f"nor KNOWN_FAILING_QUERIES (status={r.status}, error={r.error_message!r})"
      )

  _LOGGER.info(
      "TPC-DS Regression Check Summary: %d passed as expected, "
      "%d known failures matched expected error, "
      "%d unexpected pass warning(s), "
      "%d regression(s).",
      passed_queries,
      expected_failures_matched,
      len(unexpected_passes),
      len(regressions))
  if regressions:
    for msg in regressions:
      _LOGGER.error("  - %s", msg)
    raise RuntimeError(
        f"TPC-DS regression check failed with {len(regressions)} issue(s):\n" +
        "\n".join(regressions))


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main()
