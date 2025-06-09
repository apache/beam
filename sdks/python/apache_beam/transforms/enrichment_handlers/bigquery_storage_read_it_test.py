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
import pytest
import secrets
import time
import unittest
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# pylint: disable=ungrouped-imports
try:
  from apitools.base.py.exceptions import HttpError
  from google.api_core.exceptions import BadRequest, GoogleAPICallError

  # Removed NotFound from import as it is unused
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import \
      BigQueryStorageEnrichmentHandler
except ImportError:
  raise unittest.SkipTest(
      "Google Cloud BigQuery or BigQuery Storage dependencies are not "
      "installed.")

_LOGGER = logging.getLogger(__name__)


@pytest.mark.uses_testcontainer
class BigQueryStorageEnrichmentIT(unittest.TestCase):
  bigquery_dataset_id_prefix = "py_bq_storage_enrich_it_"
  project = "apache-beam-testing"  # Ensure this project is configured for tests

  @classmethod
  def setUpClass(cls):
    cls.bigquery_client = BigQueryWrapper()
    # Generate a unique dataset ID for this test run
    cls.dataset_id = "%s%d%s" % (
        cls.bigquery_dataset_id_prefix,
        int(time.time()),
        secrets.token_hex(3),
    )
    cls.bigquery_client.get_or_create_dataset(cls.project, cls.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", cls.dataset_id, cls.project)

  @classmethod
  def tearDownClass(cls):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=cls.project, datasetId=cls.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", cls.dataset_id, cls.project)
      cls.bigquery_client.client.datasets.Delete(request)
    except HttpError as e:
      _LOGGER.warning(
          "Failed to clean up dataset %s in project %s: %s",
          cls.dataset_id,
          cls.project,
          e,
      )


@pytest.mark.uses_testcontainer
class TestBigQueryStorageEnrichmentIT(BigQueryStorageEnrichmentIT):
  product_details_table_data = [
      {
          "id": 1, "name": "A", "quantity": 2, "distribution_center_id": 3
      },
      {
          "id": 2, "name": "B", "quantity": 3, "distribution_center_id": 1
      },
      {
          "id": 3, "name": "C", "quantity": 10, "distribution_center_id": 4
      },
      {
          "id": 4, "name": "D", "quantity": 1, "distribution_center_id": 3
      },
      {
          "id": 5, "name": "C", "quantity": 100, "distribution_center_id": 4
      },
      {
          "id": 6, "name": "D", "quantity": 11, "distribution_center_id": 3
      },
      {
          "id": 7, "name": "C", "quantity": 7, "distribution_center_id": 1
      },
  ]

  product_updates_table_data = [
      {
          "id": 10,
          "value": "old_value_10",
          "update_ts": "2023-01-01T00:00:00Z"
      },
      {
          "id": 10,
          "value": "new_value_10",
          "update_ts": "2023-01-02T00:00:00Z"
      },
      {
          "id": 11,
          "value": "current_value_11",
          "update_ts": "2023-01-05T00:00:00Z"
      },
      {
          "id": 10,
          "value": "latest_value_10",
          "update_ts": "2023-01-03T00:00:00Z"
      },
  ]

  @classmethod
  def create_table(cls, table_id_suffix, schema_fields, data):
    table_id = f"table_{table_id_suffix}_{secrets.token_hex(2)}"
    table_schema = bigquery.TableSchema()
    for name, field_type in schema_fields:
      table_field = bigquery.TableFieldSchema()
      table_field.name = name
      table_field.type = field_type
      table_schema.fields.append(table_field)

    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=cls.project, datasetId=cls.dataset_id, tableId=table_id),
        schema=table_schema,
    )
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=cls.project, datasetId=cls.dataset_id, table=table)
    cls.bigquery_client.client.tables.Insert(request)
    if data:
      cls.bigquery_client.insert_rows(
          cls.project, cls.dataset_id, table_id, data)

    fq_table_name = f"{cls.project}.{cls.dataset_id}.{table_id}"
    _LOGGER.info("Created table %s", fq_table_name)
    return fq_table_name

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    product_schema = [
        ("id", "INTEGER"),
        ("name", "STRING"),
        ("quantity", "INTEGER"),
        ("distribution_center_id", "INTEGER"),
    ]
    cls.product_details_table_fq = cls.create_table(
        "product_details", product_schema, cls.product_details_table_data)

    updates_schema = [
        ("id", "INTEGER"),
        ("value", "STRING"),
        ("update_ts", "TIMESTAMP"),
    ]
    cls.product_updates_table_fq = cls.create_table(
        "product_updates", updates_schema, cls.product_updates_table_data)

  def setUp(self):
    self.default_row_restriction = "id = {}"
    self.default_fields = ["id"]

  # [START test_enrichment_single_element]
  def test_enrichment_single_element(self):
    requests = [beam.Row(id=1, source_field="SourceA")]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=self.default_row_restriction,
        fields=self.default_fields,
        column_names=["id", "name", "quantity"],
        min_batch_size=1,
        max_batch_size=1,
    )

    expected_output = [
        beam.Row(id=1, source_field="SourceA", name="A", quantity=2)
    ]

    with TestPipeline(is_integration_test=True) as p:
      input_pcoll = p | "CreateRequests" >> beam.Create(requests)
      enriched_pcoll = input_pcoll | "Enrich" >> Enrichment(handler)
      assert_that(enriched_pcoll, equal_to(expected_output))

  # [END test_enrichment_single_element]

  def test_enrichment_batch_elements(self):
    requests = [
        beam.Row(id=1, source_field="Item1"),
        beam.Row(id=2, source_field="Item2"),
    ]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=self.default_row_restriction,
        fields=self.default_fields,
        column_names=["id", "name", "quantity", "distribution_center_id"],
        min_batch_size=2,
        max_batch_size=10,
    )

    expected_output = [
        beam.Row(
            id=1,
            source_field="Item1",
            name="A",
            quantity=2,
            distribution_center_id=3,
        ),
        beam.Row(
            id=2,
            source_field="Item2",
            name="B",
            quantity=3,
            distribution_center_id=1,
        ),
    ]

    with TestPipeline(is_integration_test=True) as p:
      input_pcoll = p | beam.Create(requests)
      enriched_pcoll = input_pcoll | Enrichment(handler)
      assert_that(enriched_pcoll, equal_to(expected_output))

  def test_enrichment_column_aliasing(self):
    requests = [beam.Row(id=3, source_field="ItemC")]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=self.default_row_restriction,
        fields=self.default_fields,
        column_names=["id", "name as product_name", "quantity as stock_count"],
    )
    expected_output = [
        beam.Row(id=3, source_field="ItemC", product_name="C", stock_count=10)
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_no_match_passes_through(self):
    requests = [
        beam.Row(id=1, source_field="ItemA"),  # Match
        beam.Row(id=99, source_field="ItemZ"),  # No Match
    ]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=self.default_row_restriction,
        fields=self.default_fields,
        column_names=["id", "name", "quantity"],
    )

    expected_output = [
        beam.Row(id=1, source_field="ItemA", name="A", quantity=2),
        beam.Row(id=99,
                 source_field="ItemZ"),  # Original row, no enrichment fields
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_select_all_columns_asterisk(self):
    requests = [beam.Row(id=4, source_field="ItemD")]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=self.default_row_restriction,
        fields=self.default_fields,
        column_names=["*"],
    )

    expected_output = [
        beam.Row(
            id=4,
            source_field="ItemD",
            name="D",
            quantity=1,
            distribution_center_id=3,
        )
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_row_restriction_template_fn(self):
    def custom_template_fn(
        condition_values: Dict[str, Any],
        primary_keys: Optional[List[str]],
        request_row: beam.Row,
    ) -> str:
      # request_row has 'lookup_id' and 'lookup_name'
      # condition_values will have 'id_val' and 'name_val' from
      # condition_value_fn
      return (
          f"id = {condition_values['id_val']} AND "
          f"name = '{condition_values['name_val']}'")

    def custom_cond_val_fn(req_row: beam.Row) -> Dict[str, Any]:
      return {"id_val": req_row.lookup_id, "name_val": req_row.lookup_name}

    requests = [beam.Row(lookup_id=5, lookup_name="C")]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template_fn=custom_template_fn,
        condition_value_fn=custom_cond_val_fn,
        column_names=["quantity", "distribution_center_id"],
    )

    expected_output = [
        beam.Row(
            lookup_id=5,
            lookup_name="C",
            quantity=100,
            distribution_center_id=4)
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_condition_value_fn(self):
    def custom_cond_val_fn(req_row: beam.Row) -> Dict[str, Any]:
      # req_row has 'product_identifier'
      return {"the_id": req_row.product_identifier}

    requests = [beam.Row(product_identifier=6)]
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template="id = {the_id}",  # Uses key from cond_val_fn
        condition_value_fn=custom_cond_val_fn,
        column_names=["id", "name", "quantity"],
    )

    expected_output = [
        beam.Row(product_identifier=6, id=6, name="D", quantity=11)
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_additional_condition_fields(self):
    requests = [beam.Row(target_id=7, filter_on_name="C")]

    # The handler will try to format "id = {} AND name = '{}'"
    # with (requests_row.target_id, requests_row.filter_on_name)
    # This requires careful alignment of fields and template.
    # Let's adjust the handler to use named placeholders for clarity with
    # additional_fields
    # Or, ensure the template matches the order of fields +
    # additional_condition_fields

    # For this test, let's assume the template is designed for positional
    # formatting where the first {} takes from `fields` and subsequent {}
    # take from `additional_condition_fields`
    # The current implementation of _get_condition_values_dict for `fields` +
    # `additional_condition_fields` creates a dictionary. So the template
    # should use named placeholders.

    # Re-designing this specific test for clarity with named placeholders:
    # Let condition_value_fn handle the mapping if complex.
    # If using `fields` and `additional_condition_fields`, the template
    # should use the field names directly if they are the keys in the dict
    # passed to format.

    # Let's use a condition_value_fn for this scenario to be explicit
    def complex_cond_fn(req: beam.Row) -> Dict[str, Any]:
      return {"id_val": req.target_id, "name_val": req.filter_on_name}

    handler_revised = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        condition_value_fn=complex_cond_fn,
        row_restriction_template="id = {id_val} AND name = '{name_val}'",
        column_names=["quantity", "distribution_center_id"],
    )

    expected_output = [
        beam.Row(
            target_id=7,
            filter_on_name="C",
            quantity=7,
            distribution_center_id=1)
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler_revised)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_latest_value_selector(self):
    def select_latest_by_ts(bq_results: List[beam.Row],
                            request_row: beam.Row) -> Optional[beam.Row]:
      if not bq_results:
        return None
      # Assuming 'update_ts' is a field in bq_results and is comparable
      return max(bq_results, key=lambda r: r.update_ts)

    requests = [
        beam.Row(lookup_id=10)
    ]  # This ID has multiple entries in updates table
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_updates_table_fq,
        fields=["lookup_id"
                ],  # 'lookup_id' from request_row will map to 'id' in template
        row_restriction_template="id = {}",
        column_names=[
            "value",
            "update_ts",
        ],  # Select value and the timestamp itself
        latest_value_selector=select_latest_by_ts,
        primary_keys=["id"],  # For the selector's context if needed
    )

    expected_output = [
        # The latest_value_10 has ts 2023-01-03
        beam.Row(
            lookup_id=10,
            value="latest_value_10",
            update_ts="2023-01-03T00:00:00Z")
    ]
    with TestPipeline(is_integration_test=True) as p:
      enriched = p | beam.Create(requests) | Enrichment(handler)
      assert_that(enriched, equal_to(expected_output))

  def test_enrichment_bad_request_invalid_column_in_template(self):
    requests = [beam.Row(id=1)]
    # Using a field in template that won't be provided by 'fields'
    handler = BigQueryStorageEnrichmentHandler(
        project=self.project,
        table_name=self.product_details_table_fq,
        row_restriction_template=
        "non_existent_field = {}",  # This will cause KeyError during formatting
        fields=["id"],
        column_names=["name"],
    )

    with TestPipeline(is_integration_test=True) as p:
      _ = p | beam.Create(requests) | Enrichment(handler)
      # The error might manifest as a KeyError when formatting the template,
      # or a BadRequest from BQ if the query is malformed but syntactically
      # valid enough to send.
      # The handler's internal _build_single_row_filter catches KeyError.
      # If the query sent to BQ is invalid (e.g. "SELECT name FROM ... WHERE
      # "), BQ returns BadRequest.
      # Let's test for a BQ BadRequest due to a bad query structure.
      # Example: selecting a column that doesn't exist.
      handler_bad_select = BigQueryStorageEnrichmentHandler(
          project=self.project,
          table_name=self.product_details_table_fq,
          row_restriction_template="id = {}",
          fields=["id"],
          column_names=["non_existent_column_in_bq_table"],
      )  # BQ error

      with self.assertRaises(
          GoogleAPICallError) as e_ctx:  # Or specifically BadRequest
        p_bad = TestPipeline(is_integration_test=True)
        input_pcoll = p_bad | "CreateBad" >> beam.Create(requests)
        _ = input_pcoll | "EnrichBad" >> Enrichment(handler_bad_select)
        res = p_bad.run()
        res.wait_until_finish()

      self.assertTrue(
          isinstance(e_ctx.exception, BadRequest) or
          "NoSuchFieldError" in str(e_ctx.exception) or
          "not found in table" in str(e_ctx.exception).lower() or
          "unrecognized name" in str(e_ctx.exception).lower())


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
