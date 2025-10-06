# -*- coding: utf-8 -*-
# Copyright 2024 Google LLC & Apache Software Foundation (Original License
# Header)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
BigQuery Enrichment Source Handler using the BigQuery Storage Read API
with support for field renaming via aliases in `column_names`,
additional non-key fields for filtering, dynamic row restriction templates,
experimental parallel stream reading using ThreadPoolExecutor, and custom
row selection.
"""

import concurrent.futures  # For parallel stream reading
import logging
import re
import pyarrow as pa

from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import NotFound
from google.cloud.bigquery_storage import BigQueryReadClient

try:
  from google.cloud.bigquery_storage import types
  from google.cloud.bigquery_storage.types import (
      DataFormat, ReadRowsResponse, ReadSession)
except ImportError:
  # Fallback for older versions where types might be in different location
  from google.cloud.bigquery_storage import types
  ReadRowsResponse = types.ReadRowsResponse
  ReadSession = types.ReadSession
  DataFormat = types.DataFormat

from apache_beam.pvalue import Row as BeamRow
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

# --- Configure Logging ---
logger = logging.getLogger(__name__)

# Type hints for functions
# Input functions expect beam.Row for clarity, use beam.Row.as_dict inside if
# needed
ConditionValueFn = Callable[[BeamRow], Dict[str, Any]]
# Updated RowRestrictionTemplateFn signature based on user provided code
RowRestrictionTemplateFn = Callable[
    [Dict[str, Any], Optional[List[str]], BeamRow], str]
BQRowDict = Dict[str, Any]
# Callback for selecting the "latest" or desired row from multiple BQ results
LatestValueSelectorFn = Optional[Callable[[List[BeamRow], BeamRow],
                                          Optional[BeamRow]]]

# Regex to parse "column as alias" format, ignoring case for "as"
ALIAS_REGEX = re.compile(r"^(.*?)\s+as\s+(.*)$", re.IGNORECASE)


def _validate_bigquery_metadata(
    project,
    table_name,
    row_restriction_template,
    row_restriction_template_fn,
    fields,
    condition_value_fn,
    additional_condition_fields,
):
  """Validates parameters for Storage API usage."""
  if not project:
    raise ValueError("`project` must be provided.")
  if not table_name:
    raise ValueError("`table_name` must be provided.")
  if (row_restriction_template and
      row_restriction_template_fn) or (not row_restriction_template and
                                       not row_restriction_template_fn):
    raise ValueError(
        "Provide exactly one of `row_restriction_template` or "
        "`row_restriction_template_fn`.")
  if (fields and condition_value_fn) or (not fields and not condition_value_fn):
    raise ValueError("Provide exactly one of `fields` or `condition_value_fn`.")
  if additional_condition_fields and condition_value_fn:
    raise ValueError(
        "`additional_condition_fields` cannot be used with "
        "`condition_value_fn`.")


class BigQueryStorageEnrichmentHandler(
    EnrichmentSourceHandler[Union[BeamRow, list[BeamRow]],
                            Union[BeamRow, list[BeamRow]]]):
  """Enrichment handler for Google Cloud BigQuery using the Storage Read API.
    (Refer to __init__ for full list of features and arguments)
    """
  def __init__(
      self,
      project: str,
      table_name: str,
      *,
      row_restriction_template: Optional[str] = None,
      row_restriction_template_fn: Optional[RowRestrictionTemplateFn] = None,
      fields: Optional[list[str]] = None,  # Fields for KEY and filtering
      additional_condition_fields: Optional[list[str]] = None,  # Fields ONLY
      # for filtering
      column_names: Optional[list[str]] = None,  # Columns to select + aliases
      condition_value_fn: Optional[ConditionValueFn] = None,  # Alt way to get
      # filter/key values
      min_batch_size: Optional[int] = 1,
      max_batch_size: Optional[int] = 1000,  # Batching enabled by default
      max_batch_duration_secs: Optional[int] = None,
      max_parallel_streams: Optional[int] = None,  # Max workers for
      # ThreadPoolExecutor
      max_stream_count: int = 100,  # Max streams for BigQuery Storage Read
      # --- Added latest_value_selector and primary_keys from user code ---
      latest_value_selector: LatestValueSelectorFn = None,
      primary_keys: Optional[list[str]] = None,  # --- End added parameters ---
  ):
    """
        Initializes the BigQueryStorageEnrichmentHandler.

        Args:
            project: Google Cloud project ID.
            table_name: Fully qualified BigQuery table name.
            row_restriction_template: (Optional[str]) Template string for a
                single row's filter condition. If `row_restriction_template_fn`
                is not provided, this template will be formatted with values
                from `fields` and `additional_condition_fields`.
            row_restriction_template_fn: (Optional[Callable]) Function that
                takes (condition_values_dict, primary_keys, request_row) and
                returns a fully formatted filter string or template to 
                be formatted.
            fields: (Optional[list[str]]) Input `beam.Row` field names used to
                generate the dictionary for formatting the row restriction
                template AND for generating the internal join/cache key.
            additional_condition_fields: (Optional[list[str]]) Additional input
                `beam.Row` field names used ONLY for formatting the row
                restriction template. Not part of join/cache key.
            column_names: (Optional[list[str]]) Names/aliases of columns to
                select. Supports "original_col as alias_col" format. If None,
                selects '*'.
            condition_value_fn: (Optional[Callable]) Function returning a
                dictionary for formatting row restriction template and for
                join/cache key. Takes precedence over `fields`.
            min_batch_size (Optional[int]): Minimum elements per batch.
                Defaults to 1.
            max_batch_size (Optional[int]): Maximum elements per batch.
                Defaults to 1000 for batching. Set to 1 for single element
                processing to disable batching.
            max_batch_duration_secs (Optional[int]): Maximum batch buffering
                time in seconds. Defaults to 5 seconds.
            max_parallel_streams (Optional[int]): Max worker threads for
                ThreadPoolExecutor for reading streams in parallel within a
                single `__call__`.
            max_stream_count (int): Maximum number of streams for BigQuery
                Storage Read API. Defaults to 100. Setting to 0 lets BigQuery
                decide the optimal number of streams.
            latest_value_selector: (Optional) Callback function to select the
                desired row when multiple BQ rows match a key. Takes
                `List[beam.Row]` (BQ results) and the original `beam.Row`
                (request) and returns one `beam.Row` or None.
            primary_keys: (Optional[list[str]]) Primary key fields used
                potentially by `row_restriction_template_fn` or
                `latest_value_selector`.
        """
    _validate_bigquery_metadata(
        project,
        table_name,
        row_restriction_template,
        row_restriction_template_fn,
        fields,
        condition_value_fn,
        additional_condition_fields,
    )
    self.project = project
    self.table_name = table_name
    self.row_restriction_template = row_restriction_template
    self.row_restriction_template_fn = row_restriction_template_fn
    self.fields = fields
    self.additional_condition_fields = additional_condition_fields or []
    self.condition_value_fn = condition_value_fn
    self.max_parallel_streams = max_parallel_streams
    self.max_stream_count = max_stream_count
    # --- Store new parameters ---
    self._latest_value_callback = latest_value_selector
    self.primary_keys = primary_keys
    # --- End store ---

    self._rename_map: Dict[str, str] = {}
    bq_columns_to_select_set: Set[str] = set()
    self._select_all_columns = False
    if column_names:
      for name_or_alias in column_names:
        match = ALIAS_REGEX.match(name_or_alias)
        if match:
          original_col, alias_col = (
              match.group(1).strip(),
              match.group(2).strip(),
          )
          if not original_col or not alias_col:
            raise ValueError(f"Invalid alias: '{name_or_alias}'")
          bq_columns_to_select_set.add(original_col)
          self._rename_map[original_col] = alias_col
        else:
          col = name_or_alias.strip()
          if not col:
            raise ValueError("Empty column name.")
          if col == "*":
            self._select_all_columns = True
            break
          bq_columns_to_select_set.add(col)
    else:
      self._select_all_columns = True

    key_gen_fields_set = set(self.fields or [])
    if self._select_all_columns:
      self._bq_select_columns = ["*"]
      if key_gen_fields_set:
        logger.debug(
            "Selecting all columns ('*'). Key fields %s assumed present.",
            key_gen_fields_set,
        )
    else:
      fields_to_ensure_selected = set()
      if self.fields:
        reverse_rename_map = {v: k for k, v in self._rename_map.items()}
        for field in self.fields:
          original_name = reverse_rename_map.get(field, field)
          fields_to_ensure_selected.add(original_name)
      # Ensure primary keys (if defined for callback use) are selected if not
      # already
      if self.primary_keys:
        for pk_field in self.primary_keys:
          original_pk_name = {
              v: k
              for k, v in self._rename_map.items()
          }.get(pk_field, pk_field)
          fields_to_ensure_selected.add(original_pk_name)

      final_select_set = bq_columns_to_select_set.union(
          fields_to_ensure_selected)
      self._bq_select_columns = sorted(list(final_select_set))
      if not self._bq_select_columns:
        raise ValueError("No columns determined for selection.")

    logger.info(
        "Handler Initialized. Selecting BQ Columns: %s. Renaming map: %s",
        self._bq_select_columns,
        self._rename_map,
    )

    self._batching_kwargs = {}
    # Set defaults for optional parameters
    min_batch_size = min_batch_size or 1
    max_batch_size = max_batch_size or 1000
    max_batch_duration_secs = max_batch_duration_secs or 5

    if max_batch_size > 1:
      self._batching_kwargs["min_batch_size"] = min_batch_size
      self._batching_kwargs["max_batch_size"] = max_batch_size
      self._batching_kwargs["max_batch_duration_secs"] = max_batch_duration_secs
    else:
      self._batching_kwargs["min_batch_size"] = 1
      self._batching_kwargs["max_batch_size"] = 1

    self._client: Optional[BigQueryReadClient] = None
    self._arrow_schema: Optional[pa.Schema] = None

  def __enter__(self):
    if not self._client:
      self._client = BigQueryReadClient()
      logger.info("BigQueryStorageEnrichmentHandler: Client created.")
    self._arrow_schema = None

  def _get_condition_values_dict(self,
                                 req: BeamRow) -> Optional[Dict[str, Any]]:
    try:
      if self.condition_value_fn:
        values_dict = self.condition_value_fn(req)
        if values_dict is None or any(v is None for v in values_dict.values()):
          logger.warning(
              "condition_value_fn returned None or None value(s). "
              "Skipping: %s. Values: %s",
              req,
              values_dict,
          )
          return None
        return values_dict
      elif self.fields is not None:
        req_dict = req._asdict()
        values_dict = {}
        all_req_fields = (self.fields or []) + self.additional_condition_fields
        for field in all_req_fields:
          # User's provided logic for row_restriction_template_fn handling:
          if not self.row_restriction_template_fn:
            if field not in req_dict or req_dict[field] is None:
              logger.warning(
                  "Input row missing field '%s' or None (needed "
                  "for filter). Skipping: %s",
                  field,
                  req,
              )
              return None
          values_dict[field] = req_dict.get(field)  # Use get for safety
        return values_dict
      else:
        raise ValueError(
            "Internal error: Neither fields nor condition_value_fn.")
    except AttributeError:  # Specifically for _asdict()
      logger.error(
          "Failed to call _asdict() on element. Type: %s. Element: "
          "%s. Ensure input is beam.Row.",
          type(req),
          req,
      )
      return None
    except Exception as e:
      logger.error(
          "Error getting condition values for row %s: %s",
          req,
          e,
          exc_info=True)
      return None

  def _build_single_row_filter(
      self, req_row: BeamRow, condition_values_dict: Dict[str, Any]) -> str:
    """Builds the filter string part for a single row."""
    try:
      if self.row_restriction_template_fn:
        # User's provided signature for row_restriction_template_fn
        template_or_filter = self.row_restriction_template_fn(
            condition_values_dict, self.primary_keys, req_row)
        if not isinstance(template_or_filter, str):
          raise TypeError(
              "row_restriction_template_fn must return a string "
              "(filter or template to be formatted)")
        # Assuming if it takes condition_values_dict, it might be returning
        # the final filter or a template. If it's a template, it still needs
        # .format(). For now, assume it's a template that might still need
        # formatting OR the final filter string. Let's assume it's the final
        # filter string as per user's code.
        # Directly return what the user's function gives.
        return template_or_filter
      elif self.row_restriction_template:
        return self.row_restriction_template.format(**condition_values_dict)
      else:
        raise ValueError(
            "Internal Error: No template or template function available.")
    except KeyError as e:  # if user's fn returns template and format fails
      raise ValueError(
          f"Placeholder {{{e}}} in template not found in "
          f"condition values: {condition_values_dict.keys()}")
    except Exception as e:
      logger.error(
          "Error building filter for row %s with values %s: %s",
          req_row,
          condition_values_dict,
          e,
          exc_info=True,
      )
      return ""

  def _apply_renaming(self, bq_row_dict: BQRowDict) -> BQRowDict:
    if not self._rename_map:
      return bq_row_dict
    return {self._rename_map.get(k, k): v for k, v in bq_row_dict.items()}

  def _arrow_to_dicts(self, response: ReadRowsResponse) -> Iterator[BQRowDict]:
    # Now uses self._arrow_schema directly
    if response.arrow_record_batch:
      if not self._arrow_schema:
        logger.error(
            "Cannot process Arrow batch: Schema not "
            "available/cached in handler.")
        return
      try:
        serialized_batch = response.arrow_record_batch.serialized_record_batch
        record_batch = pa.ipc.read_record_batch(
            pa.py_buffer(serialized_batch), self._arrow_schema)
        arrow_table = pa.Table.from_batches([record_batch])
        yield from arrow_table.to_pylist()
      except Exception as e:
        logger.error(
            "Error converting Arrow batch to dicts: %s", e, exc_info=True)

  def _execute_storage_read(self, combined_row_filter: str) -> List[BQRowDict]:
    if not self._client:
      self.__enter__()
    if not self._client:
      raise RuntimeError("BQ Client failed to initialize.")
    if not combined_row_filter:
      logger.warning("Empty filter, skipping BQ read.")
      return []

    try:
      table_project, dataset_id, table_id = self.table_name.split(".")
    except ValueError:
      raise ValueError(
          f"Invalid table_name: '{self.table_name}'. Expected "
          "'project.dataset.table'.")
    parent_project = self.project
    table_resource = (
        f"projects/{table_project}/datasets/{dataset_id}/tables/{table_id}")

    session = None
    try:
      # TODO: Improve max_stream_count to be dynamic based on input size,
      # data volume, and query complexity for optimal performance
      req = {
          "parent": f"projects/{parent_project}",
          "read_session": ReadSession(
              table=table_resource,
              data_format=DataFormat.ARROW,
              read_options=ReadSession.TableReadOptions(
                  row_restriction=combined_row_filter,
                  selected_fields=self._bq_select_columns,
              ),
          ),
          "max_stream_count": self.max_stream_count,
      }
      session = self._client.create_read_session(request=req)
      logger.debug(
          "Session with %s streams. Filter: %s",
          len(session.streams),
          combined_row_filter,
      )
      if session.streams and session.arrow_schema:
        if not self._arrow_schema:
          self._arrow_schema = pa.ipc.read_schema(
              pa.py_buffer(session.arrow_schema.serialized_schema))
          logger.debug("Deserialized Arrow schema for current call.")
      elif session.streams:
        logger.error("Session has streams but no schema.")
        return []
    except (BadRequest, NotFound, GoogleAPICallError) as e:
      logger.error(
          "BQ API error creating session. Filter: '%s'. Error: %s",
          combined_row_filter,
          e,
      )
      return []
    except Exception as e:
      logger.error(
          "Unexpected error creating session. Filter: '%s'. Error: %s",
          combined_row_filter,
          e,
          exc_info=True,
      )
      return []

    if not session or not session.streams:
      logger.warning("No streams for filter: %s", combined_row_filter)
      return []

    def _read_single_stream_worker(stream_name: str) -> List[BQRowDict]:
      worker_results = []
      if not self._client or not self._arrow_schema:
        logger.error("Stream %s: Client/schema missing in worker.", stream_name)
        return worker_results
      try:
        reader = self._client.read_rows(stream_name)
        for response in reader:
          worker_results.extend(self._arrow_to_dicts(response))  # Uses
          # self._arrow_schema
      except Exception as e:
        logger.error(
            "Error reading stream %s in worker: %s",
            stream_name,
            e,
            exc_info=True,
        )
      return worker_results

    all_bq_rows_original_keys = []
    num_api_streams = len(session.streams)
    max_workers = num_api_streams
    if self.max_parallel_streams is not None and self.max_parallel_streams > 0:
      max_workers = min(num_api_streams, self.max_parallel_streams)
    if max_workers <= 0:
      max_workers = 1
    logger.debug(
        "Reading %s API streams using %s threads.",
        num_api_streams,
        max_workers)
    futures = []
    try:
      with concurrent.futures.ThreadPoolExecutor(
          max_workers=max_workers) as executor:
        for stream in session.streams:
          futures.append(
              executor.submit(_read_single_stream_worker, stream.name))
        for future in concurrent.futures.as_completed(futures):
          try:
            all_bq_rows_original_keys.extend(future.result())
          except Exception as e:
            logger.error("Error processing future result: %s", e, exc_info=True)
    except Exception as pool_error:
      logger.error("ThreadPool error: %s", pool_error, exc_info=True)
    logger.debug("Fetched %s rows from BQ.", len(all_bq_rows_original_keys))
    return all_bq_rows_original_keys

  def create_row_key(self,
                     row: BeamRow,
                     is_bq_result: bool = False) -> Optional[tuple]:
    try:
      if self.condition_value_fn:
        key_values_dict = self.condition_value_fn(row)
      elif self.fields is not None:
        row_dict = row._asdict()  # Assumes row is BeamRow

        # If this is a BQ result row with aliased columns, map field names
        # to aliases
        fields_to_use = self.fields
        if is_bq_result and self._rename_map:
          # Map original field names to their aliases
          fields_to_use = [self._rename_map.get(f, f) for f in self.fields]

        key_values_dict = {
            # Use original field name as key, but get value using appropriate
            # field name
            self.fields[i]: row_dict[field_name]
            for i, field_name in enumerate(fields_to_use)
            if field_name in row_dict and row_dict[field_name] is not None
        }
        if len(key_values_dict) != len(self.fields):  # Ensure all key fields
          # found and not None
          logger.debug(
              "Row missing key field(s) or None. Cannot generate key: %s", row)
          return None
      else:
        raise ValueError(
            "Internal error: Neither fields nor condition_value_fn for key.")
      if key_values_dict is None:
        return None
      return tuple(sorted(key_values_dict.items()))
    except AttributeError:
      logger.error(
          "Failed _asdict() for key gen. Type: %s. Ensure input is beam.Row.",
          type(row),
      )
      return None
    except Exception as e:
      logger.error("Error generating key for row %s: %s", row, e, exc_info=True)
      return None

  def _process_bq_results_for_batch(
      self,
      bq_results_list_orig_keys: List[BQRowDict]) -> Dict[tuple, List[BeamRow]]:
    """Process BQ results and create a mapping from keys to renamed rows."""
    bq_results_key_map: Dict[tuple, List[BeamRow]] = {}
    for bq_row_dict_orig_keys in bq_results_list_orig_keys:
      try:
        renamed_bq_row_dict = self._apply_renaming(bq_row_dict_orig_keys)
        bq_row_renamed_keys_temp = BeamRow(**renamed_bq_row_dict)
        resp_key = self.create_row_key(
            bq_row_renamed_keys_temp, is_bq_result=True)
        if resp_key:
          if resp_key not in bq_results_key_map:
            bq_results_key_map[resp_key] = []
          bq_results_key_map[resp_key].append(bq_row_renamed_keys_temp)
      except Exception as e:
        logger.warning(
            "Error processing BQ response row %s: %s. Cannot map.",
            bq_row_dict_orig_keys,
            e,
        )
    return bq_results_key_map

  def _select_response_row(
      self, matching_bq_rows: List[BeamRow], req_row: BeamRow) -> BeamRow:
    """Select the appropriate response row from matching BQ rows."""
    if not matching_bq_rows:
      return BeamRow()

    if self._latest_value_callback:
      try:
        return (
            self._latest_value_callback(matching_bq_rows, req_row) or BeamRow())
      except Exception as cb_error:
        logger.error(
            "Error in latest_value_selector: %s. Using first BQ row.",
            cb_error,
            exc_info=True,
        )
        return matching_bq_rows[0]
    else:
      return matching_bq_rows[0]  # Default to first

  def _process_batch_request(
      self, request: list[BeamRow]) -> List[Tuple[BeamRow, BeamRow]]:
    """
        Process a batch of requests efficiently using a single BigQuery query.

        This method optimizes batch processing by:
        1. Deduplicating requests with identical keys
        2. Building individual row filters for each unique request
        3. Combining all filters into a single OR query to minimize API calls
        4. Mapping BigQuery results back to original requests

        Args:
            request: List of BeamRow objects to enrich with BigQuery data

        Returns:
            List of tuples containing (original_request, enriched_response)
            pairs
        """
    # Initialize collections for processing
    batch_responses: List[Tuple[BeamRow, BeamRow]] = []  # Final results
    requests_map: Dict[tuple, BeamRow] = {}  # Unique key -> request mapping
    single_row_filters: List[str] = []  # Individual SQL filter conditions

    # Phase 1: Process each request row and build individual filters
    for req_row in request:
      # Extract condition values (e.g., key fields) from the request row
      condition_values = self._get_condition_values_dict(req_row)
      if condition_values is None:
        # Missing required fields - add empty response and skip processing
        batch_responses.append((req_row, BeamRow()))
        continue

      # Generate a unique key for this request (used for deduplication)
      req_key = self.create_row_key(req_row)
      if req_key is None:
        # Cannot generate key - add empty response and skip processing
        batch_responses.append((req_row, BeamRow()))
        continue

      # Handle duplicate detection and filter building
      if req_key not in requests_map:
        # New unique request - store it and build its filter
        requests_map[req_key] = req_row
        single_filter = self._build_single_row_filter(req_row, condition_values)
        if single_filter:
          # Wrap in parentheses for safe OR combination
          single_row_filters.append(f"({single_filter})")
        else:
          # Filter generation failed - add empty response
          batch_responses.append((req_row, BeamRow()))
          del requests_map[req_key]  # Clean up
      else:
        # Duplicate key detected - log warning and return empty response
        logger.warning(
            "Duplicate key '%s' in batch. Processing first instance.", req_key)
        batch_responses.append((req_row, BeamRow()))

    # Phase 2: Execute combined BigQuery query if we have valid filters
    bq_results_key_map: Dict[tuple,
                             List[BeamRow]] = {}  # Key -> BQ results mapping
    if single_row_filters:
      # Combine all individual filters with OR to create single query
      # Example: "(id = 1) OR (id = 2) OR (id = 3)"
      combined_filter = " OR ".join(single_row_filters)

      # Execute single BigQuery Storage Read API call
      bq_results_list_orig_keys = self._execute_storage_read(combined_filter)

      # Process raw BigQuery results: apply column renaming and group by key
      bq_results_key_map = self._process_bq_results_for_batch(
          bq_results_list_orig_keys)

    # Phase 3: Match BigQuery results back to original requests
    for req_key, req_row in requests_map.items():
      # Find all BigQuery rows that match this request's key
      matching_bq_rows = bq_results_key_map.get(req_key, [])

      # Select the best response row (first match or custom selector result)
      selected_response_row = self._select_response_row(
          matching_bq_rows, req_row)

      # Add the (request, response) pair to final results
      batch_responses.append((req_row, selected_response_row))

    return batch_responses

  def _process_single_request(self,
                              request: BeamRow) -> Tuple[BeamRow, BeamRow]:
    """Process a single request using a direct BQ query."""
    req_row = request
    condition_values = self._get_condition_values_dict(req_row)
    if condition_values is None:
      return (req_row, BeamRow())
    single_filter = self._build_single_row_filter(req_row, condition_values)
    if not single_filter:
      return (req_row, BeamRow())
    bq_results_orig_keys = self._execute_storage_read(single_filter)
    response_row = BeamRow()
    if bq_results_orig_keys:
      # For single request, apply selector if provided, else take first
      renamed_bq_rows = [
          BeamRow(**self._apply_renaming(d)) for d in bq_results_orig_keys
      ]
      if self._latest_value_callback and renamed_bq_rows:
        try:
          response_row = (
              self._latest_value_callback(renamed_bq_rows, req_row) or
              BeamRow())
        except Exception as cb_error:
          logger.error(
              "Error in latest_value_selector for single req: %s. "
              "Using first BQ row.",
              cb_error,
              exc_info=True,
          )
          response_row = renamed_bq_rows[0]
      elif renamed_bq_rows:
        response_row = renamed_bq_rows[0]
      if len(bq_results_orig_keys) > 1 and not (
          self._latest_value_callback and
          response_row != BeamRow()):  # Log if multiple and
        # default/callback didn't pick one specifically
        logger.warning(
            "Single request -> %s BQ rows. Used selected/first. Filter:'%s'",
            len(bq_results_orig_keys),
            single_filter,
        )
    return (req_row, response_row)

  def __call__(  # type: ignore[override]
      self, request: Union[BeamRow, list[BeamRow]], *args, **kwargs
  ) -> Union[Tuple[BeamRow, BeamRow], List[Tuple[BeamRow, BeamRow]]]:
    self._arrow_schema = None  # Reset schema

    if isinstance(request, list):
      return self._process_batch_request(request)
    else:
      return self._process_single_request(request)

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._client:
      logger.info("BigQueryStorageEnrichmentHandler: Releasing client.")
      self._client = None

  def get_cache_key(self, request: Union[BeamRow, list[BeamRow]]) -> str:
    # TODO: Add proper caching functionality with TTL, cache size limits,
    # and configurable cache policies to improve performance and reduce
    # BigQuery API calls for repeated requests.
    if isinstance(request, list):
      # For batch requests, create a composite key
      keys = [
          str(self.create_row_key(req) or "__invalid_key__") for req in request
      ]
      return "|".join(keys)
    else:
      return str(self.create_row_key(request) or "__invalid_key__")

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    return self._batching_kwargs
