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

"""Unit tests for BigQuery compatibility shims and legacy client emulation.

These tests verify the compatibility models, monkey-patches, and emulated
interfaces in ``apache_beam.io.gcp.bigquery_compat``.

.. note::
   This test suite is intended to be completely removed along with
   ``bigquery_compat.py`` in a future Beam release when apitools is removed.
"""

# pytype: skip-file

import logging
import pickle
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.io.gcp import bigquery_compat
from apache_beam.io.gcp import bigquery_tools

try:
  from google.cloud import bigquery as gcp_bigquery
except ImportError:
  gcp_bigquery = None

try:
  from apache_beam.io.gcp.internal.clients import bigquery as apitools_bigquery
except ImportError:
  apitools_bigquery = None


class TestJobReferenceCompatibility(unittest.TestCase):
  def test_init_camel_case(self):
    ref = bigquery_compat.JobReference(
        jobId="test-job", projectId="test-proj", location="US")
    self.assertEqual(ref.jobId, "test-job")
    self.assertEqual(ref.job_id, "test-job")
    self.assertEqual(ref.projectId, "test-proj")
    self.assertEqual(ref.project, "test-proj")
    self.assertEqual(ref.project_id, "test-proj")
    self.assertEqual(ref.location, "US")

  def test_init_snake_case(self):
    ref = bigquery_compat.JobReference(
        job_id="test-job", project="test-proj", location="EU")
    self.assertEqual(ref.jobId, "test-job")
    self.assertEqual(ref.job_id, "test-job")
    self.assertEqual(ref.projectId, "test-proj")
    self.assertEqual(ref.project, "test-proj")
    self.assertEqual(ref.project_id, "test-proj")
    self.assertEqual(ref.location, "EU")

  def test_init_with_project_id(self):
    ref = bigquery_compat.JobReference(
        job_id="test-job", project_id="test-proj", location="EU")
    self.assertEqual(ref.jobId, "test-job")
    self.assertEqual(ref.job_id, "test-job")
    self.assertEqual(ref.projectId, "test-proj")
    self.assertEqual(ref.project, "test-proj")
    self.assertEqual(ref.project_id, "test-proj")

  def test_attribute_setters(self):
    ref = bigquery_compat.JobReference()
    ref.job_id = "j1"
    ref.project = "p1"
    self.assertEqual(ref.jobId, "j1")
    self.assertEqual(ref.job_id, "j1")
    self.assertEqual(ref.projectId, "p1")
    self.assertEqual(ref.project, "p1")
    self.assertEqual(ref.project_id, "p1")
    ref.project_id = "p2"
    self.assertEqual(ref.projectId, "p2")
    self.assertEqual(ref.project, "p2")
    self.assertEqual(ref.project_id, "p2")

  def test_equality_with_custom_and_apitools(self):
    ref1 = bigquery_compat.JobReference(
        job_id="j1", project="p1", location="US")
    ref2 = bigquery_compat.JobReference(
        jobId="j1", projectId="p1", location="US")
    ref3 = bigquery_compat.JobReference(
        jobId="j2", projectId="p1", location="US")
    self.assertEqual(ref1, ref2)
    self.assertNotEqual(ref1, ref3)
    self.assertEqual(hash(ref1), hash(ref2))

    if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                                 "JobReference"):
      ap_ref = apitools_bigquery.JobReference(
          jobId="j1", projectId="p1", location="US")
      self.assertEqual(ref1, ap_ref)
      self.assertEqual(ap_ref, ref1)

  def test_equality_type_safety(self):
    empty_ref = bigquery_compat.JobReference()
    self.assertNotEqual(empty_ref, None)
    self.assertNotEqual(empty_ref, 123)
    self.assertNotEqual(empty_ref, "")
    self.assertNotEqual(empty_ref, {})

    ref = bigquery_compat.JobReference(project="p1")
    table_ref = bigquery_compat.TableReference(
        projectId="p1", datasetId="d1", tableId="t1")
    self.assertNotEqual(ref, table_ref)
    self.assertNotEqual(empty_ref, table_ref)

  def test_pickle_and_coder_roundtrip(self):
    ref = bigquery_compat.JobReference(job_id="j1", project="p1", location="US")
    pickled = pickle.dumps(ref)
    unpickled = pickle.loads(pickled)
    self.assertEqual(ref, unpickled)
    self.assertEqual(unpickled.jobId, "j1")
    self.assertEqual(unpickled.projectId, "p1")
    self.assertEqual(unpickled.location, "US")

    coder = beam.coders.FastPrimitivesCoder()
    encoded = coder.encode(ref)
    decoded = coder.decode(encoded)
    self.assertEqual(ref, decoded)


class TestTableAndDatasetReferenceCompatibility(unittest.TestCase):
  @unittest.skipIf(gcp_bigquery is None, "google-cloud-bigquery not installed")
  def test_table_reference_property_mutability(self):
    ds = gcp_bigquery.DatasetReference("p1", "d1")
    table = gcp_bigquery.TableReference(ds, "t1")
    table.tableId = "t2"
    self.assertEqual(table.tableId, "t2")
    self.assertEqual(table.table_id, "t2")
    table.datasetId = "d2"
    self.assertEqual(table.datasetId, "d2")
    self.assertEqual(table.dataset_id, "d2")
    table.projectId = "p2"
    self.assertEqual(table.projectId, "p2")
    self.assertEqual(table.project, "p2")

  def test_table_reference_compat_model(self):
    table = bigquery_compat._TableReferenceCompat(
        projectId="p1", datasetId="d1", tableId="t1")
    self.assertEqual(table.projectId, "p1")
    self.assertEqual(table.project, "p1")
    self.assertEqual(table.project_id, "p1")
    self.assertEqual(table.datasetId, "d1")
    self.assertEqual(table.dataset_id, "d1")
    self.assertEqual(table.tableId, "t1")
    self.assertEqual(table.table_id, "t1")
    table.tableId = "t2"
    self.assertEqual(table.tableId, "t2")
    self.assertEqual(table.table_id, "t2")
    table.datasetId = "d2"
    self.assertEqual(table.datasetId, "d2")
    self.assertEqual(table.dataset_id, "d2")
    table.projectId = "p2"
    self.assertEqual(table.projectId, "p2")
    self.assertEqual(table.project, "p2")
    self.assertEqual(table.project_id, "p2")

  def test_dataset_reference_compat_model(self):
    ds = bigquery_compat._DatasetReferenceCompat(projectId="p1", datasetId="d1")
    self.assertEqual(ds.projectId, "p1")
    self.assertEqual(ds.project, "p1")
    self.assertEqual(ds.project_id, "p1")
    self.assertEqual(ds.datasetId, "d1")
    self.assertEqual(ds.dataset_id, "d1")
    ds.projectId = "p2"
    self.assertEqual(ds.projectId, "p2")
    self.assertEqual(ds.project, "p2")
    ds.datasetId = "d2"
    self.assertEqual(ds.datasetId, "d2")
    self.assertEqual(ds.dataset_id, "d2")

  def test_to_gcp_dataset_ref_colon_format(self):
    ds_ref = bigquery_compat._to_gcp_dataset_ref("my-project:my_dataset")
    self.assertEqual(ds_ref.project, "my-project")
    self.assertEqual(ds_ref.dataset_id, "my_dataset")
    self.assertEqual(ds_ref.projectId, "my-project")
    self.assertEqual(ds_ref.datasetId, "my_dataset")

  def test_to_gcp_dataset_ref_domain_scoped(self):
    ds_ref1 = bigquery_compat._to_gcp_dataset_ref(
        "google.com:clouddfe:my_dataset")
    self.assertEqual(ds_ref1.project, "google.com:clouddfe")
    self.assertEqual(ds_ref1.dataset_id, "my_dataset")

    ds_ref2 = bigquery_compat._to_gcp_dataset_ref(
        "google.com:clouddfe.my_dataset")
    self.assertEqual(ds_ref2.project, "google.com:clouddfe")
    self.assertEqual(ds_ref2.dataset_id, "my_dataset")

  def test_table_reference_from_string_default_project(self):
    t_ref = bigquery_compat._TableReferenceCompat.from_string(
        "my_ds.my_tbl", default_project="default-proj")
    self.assertEqual(t_ref.projectId, "default-proj")
    self.assertEqual(t_ref.datasetId, "my_ds")
    self.assertEqual(t_ref.tableId, "my_tbl")

  def test_dataset_reference_from_string_domain_scoped(self):
    ds_ref = bigquery_compat._DatasetReferenceCompat.from_string(
        "google.com:clouddfe:my_dataset")
    self.assertEqual(ds_ref.projectId, "google.com:clouddfe")
    self.assertEqual(ds_ref.datasetId, "my_dataset")

  def test_table_field_schema_compat(self):
    f = bigquery_compat._TableFieldSchemaCompat(
        name="age", type="INTEGER", mode="REQUIRED")
    self.assertEqual(f.name, "age")
    self.assertEqual(f.type, "INTEGER")
    self.assertEqual(f.mode, "REQUIRED")

  def test_table_schema_compat(self):
    f1 = bigquery_compat._TableFieldSchemaCompat(name="id", type="INTEGER")
    f2 = bigquery_compat._TableFieldSchemaCompat(name="val", type="STRING")
    s = bigquery_compat._TableSchemaCompat([f1, f2])
    self.assertEqual(len(s.fields), 2)
    self.assertEqual(s.fields[0].name, "id")
    self.assertEqual(s.fields[1].name, "val")


class TestJobConfigCompatibility(unittest.TestCase):
  def test_load_job_config_camel_case_properties(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    config = gcp_bigquery.LoadJobConfig(
        schemaUpdateOptions=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        ignoreUnknownValues=True,
        maxBadRecords=10,
        nullMarker="\\N",
        fieldDelimiter="\t",
        skipLeadingRows=1,
        allowJaggedRows=True,
        allowQuotedNewlines=True,
        decimalTargetTypes=["NUMERIC"],
        useAvroLogicalTypes=True,
    )
    self.assertEqual(
        config.schemaUpdateOptions,
        ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"])
    self.assertEqual(
        config.schema_update_options,
        ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"])
    self.assertTrue(config.ignoreUnknownValues)
    self.assertTrue(config.ignore_unknown_values)
    self.assertEqual(config.maxBadRecords, 10)
    self.assertEqual(config.max_bad_records, 10)
    self.assertEqual(config.nullMarker, "\\N")
    self.assertEqual(config.null_marker, "\\N")
    self.assertEqual(config.fieldDelimiter, "\t")
    self.assertEqual(config.field_delimiter, "\t")
    self.assertEqual(config.skipLeadingRows, 1)
    self.assertEqual(config.skip_leading_rows, 1)
    self.assertTrue(config.allowJaggedRows)
    self.assertTrue(config.allow_jagged_rows)
    self.assertTrue(config.allowQuotedNewlines)
    self.assertTrue(config.allow_quoted_newlines)
    self.assertEqual(set(config.decimalTargetTypes), {"NUMERIC"})
    self.assertEqual(set(config.decimal_target_types), {"NUMERIC"})
    self.assertTrue(config.useAvroLogicalTypes)
    self.assertTrue(config.use_avro_logical_types)

  def test_query_job_config_camel_case_properties(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    config = gcp_bigquery.QueryJobConfig(
        schemaUpdateOptions=["ALLOW_FIELD_ADDITION"],
        useLegacySql=True,
        flattenResults=False,
        allowLargeResults=True,
        maximumBytesBilled=1000000,
    )
    self.assertEqual(config.schemaUpdateOptions, ["ALLOW_FIELD_ADDITION"])
    self.assertEqual(config.schema_update_options, ["ALLOW_FIELD_ADDITION"])
    self.assertTrue(config.useLegacySql)
    self.assertTrue(config.use_legacy_sql)
    self.assertFalse(config.flattenResults)
    self.assertFalse(config.flatten_results)
    self.assertTrue(config.allowLargeResults)
    self.assertTrue(config.allow_large_results)
    self.assertEqual(config.maximumBytesBilled, 1000000)
    self.assertEqual(config.maximum_bytes_billed, 1000000)

  def test_insert_load_job_with_none_labels(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.MagicMock(spec=gcp_bigquery.Client)
    mock_job = mock.MagicMock()
    mock_job.job_id = "load_job_id"
    mock_job.project = "test-project"
    mock_job.location = "US"
    client.load_table_from_uri.return_value = mock_job

    wrapper = bigquery_tools.BigQueryWrapper(client)
    job_ref = wrapper._insert_load_job(
        project_id="test-project",
        job_id="load_job_id",
        table_reference="test-project:test_dataset.test_table",
        source_uris=["gs://test-bucket/test.csv"],
        job_labels=None,
    )
    self.assertEqual(job_ref.jobId, "load_job_id")
    client.load_table_from_uri.assert_called_once()
    called_config = client.load_table_from_uri.call_args.kwargs["job_config"]
    self.assertEqual(called_config.labels, {})

  def test_insert_copy_job_with_none_labels(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.MagicMock(spec=gcp_bigquery.Client)
    mock_job = mock.MagicMock()
    mock_job.job_id = "copy_job_id"
    mock_job.project = "test-project"
    mock_job.location = "US"
    client.copy_table.return_value = mock_job

    wrapper = bigquery_tools.BigQueryWrapper(client)
    job_ref = wrapper._insert_copy_job(
        project_id="test-project",
        job_id="copy_job_id",
        from_table_reference="test-project:test_dataset.src_table",
        to_table_reference="test-project:test_dataset.dst_table",
        job_labels=None,
    )
    self.assertEqual(job_ref.jobId, "copy_job_id")
    client.copy_table.assert_called_once()
    called_config = client.copy_table.call_args.kwargs["job_config"]
    self.assertEqual(called_config.labels, {})

  def test_perform_extract_job_with_none_labels(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.MagicMock(spec=gcp_bigquery.Client)
    mock_job = mock.MagicMock()
    mock_job.job_id = "extract_job_id"
    mock_job.project = "test-project"
    mock_job.location = "US"
    client.extract_table.return_value = mock_job

    wrapper = bigquery_tools.BigQueryWrapper(client)
    job_ref = wrapper.perform_extract_job(
        destination="gs://test-bucket/output.csv",
        job_id="extract_job_id",
        table_reference="test-project:test_dataset.src_table",
        destination_format="CSV",
        job_labels=None,
    )
    self.assertEqual(job_ref.jobId, "extract_job_id")
    client.extract_table.assert_called_once()
    called_config = client.extract_table.call_args.kwargs["job_config"]
    self.assertEqual(called_config.labels, {})

  def test_labels_setter_clears_on_none(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    # Table labels
    table = gcp_bigquery.Table("test-project.dataset.table")
    table.labels = {"initial": "label"}
    self.assertEqual(table.labels, {"initial": "label"})
    table.labels = None
    self.assertEqual(table.labels, {})

    # Dataset labels
    ds = gcp_bigquery.Dataset("test-project.dataset")
    ds.labels = {"initial": "label"}
    self.assertEqual(ds.labels, {"initial": "label"})
    ds.labels = None
    self.assertEqual(ds.labels, {})

    # Job configs labels
    for config_cls in (
        gcp_bigquery.QueryJobConfig,
        gcp_bigquery.LoadJobConfig,
        gcp_bigquery.CopyJobConfig,
        gcp_bigquery.ExtractJobConfig,
    ):
      cfg = config_cls(labels={"initial": "label"})
      self.assertEqual(cfg.labels, {"initial": "label"})
      cfg.labels = None
      self.assertEqual(cfg.labels, {})

  def test_table_partitioning_property_setters(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    table = gcp_bigquery.Table("test-project.dataset.table")
    tp = gcp_bigquery.TimePartitioning(type_="DAY")
    table.timePartitioning = tp
    self.assertEqual(table.timePartitioning, tp)
    self.assertEqual(table.time_partitioning, tp)

    rp = gcp_bigquery.RangePartitioning(field="id")
    table.rangePartitioning = rp
    self.assertEqual(table.rangePartitioning, rp)
    self.assertEqual(table.range_partitioning, rp)


class TestSchemaConversionCompatibility(unittest.TestCase):
  def test_to_table_schema_nested_records(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    schema = [
        gcp_bigquery.SchemaField(
            "person",
            "RECORD",
            mode="NULLABLE",
            fields=[
                gcp_bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                gcp_bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
                gcp_bigquery.SchemaField(
                    "address",
                    "RECORD",
                    fields=[
                        gcp_bigquery.SchemaField(
                            "city", "STRING", mode="NULLABLE"),
                    ]),
            ]),
    ]
    table_schema = bigquery_compat._to_table_schema(schema)
    self.assertEqual(len(table_schema.fields), 1)
    person = table_schema.fields[0]
    self.assertEqual(person.name, "person")
    self.assertEqual(person.type, "RECORD")
    self.assertEqual(len(person.fields), 3)
    self.assertEqual(person.fields[0].name, "name")
    self.assertEqual(person.fields[0].type, "STRING")
    self.assertEqual(person.fields[1].name, "age")
    self.assertEqual(person.fields[1].type, "INTEGER")
    self.assertEqual(person.fields[2].name, "address")
    self.assertEqual(person.fields[2].type, "RECORD")
    self.assertEqual(len(person.fields[2].fields), 1)
    self.assertEqual(person.fields[2].fields[0].name, "city")
    self.assertEqual(person.fields[2].fields[0].type, "STRING")

  def test_to_table_schema_dict(self):
    dict_schema = {
        "fields": [
            {
                "name": "id", "type": "INTEGER", "mode": "REQUIRED"
            },
            {
                "name": "val", "type": "STRING", "mode": "NULLABLE"
            },
        ]
    }
    table_schema = bigquery_compat._to_table_schema(dict_schema)
    self.assertEqual(len(table_schema.fields), 2)
    self.assertEqual(table_schema.fields[0].name, "id")
    self.assertEqual(table_schema.fields[0].type, "INTEGER")
    self.assertEqual(table_schema.fields[1].name, "val")
    self.assertEqual(table_schema.fields[1].type, "STRING")


class TestClientCompatibility(unittest.TestCase):
  def test_job_stats_referenced_tables(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    mock_job = mock.MagicMock(spec=gcp_bigquery.QueryJob)
    mock_job.job_id = "query_job_id"
    mock_job.project = "test-project"
    mock_job.location = "US"
    t1 = gcp_bigquery.TableReference.from_string("test-project.dataset.table1")
    t2 = gcp_bigquery.TableReference.from_string("test-project.dataset.table2")
    mock_job.referenced_tables = [t1, t2]

    stats = bigquery_compat._JobStatsCompat(mock_job)
    ref_tables = stats.referencedTables
    self.assertEqual(len(ref_tables), 2)
    self.assertEqual(ref_tables[0].projectId, "test-project")
    self.assertEqual(ref_tables[0].datasetId, "dataset")
    self.assertEqual(ref_tables[0].tableId, "table1")
    self.assertEqual(ref_tables[1].projectId, "test-project")
    self.assertEqual(ref_tables[1].datasetId, "dataset")
    self.assertEqual(ref_tables[1].tableId, "table2")

  def test_client_tables_compat_insert_labels_and_metadata(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.MagicMock(spec=gcp_bigquery.Client)
    client.project = "test-project"
    created_table_mock = mock.MagicMock(spec=gcp_bigquery.Table)
    client.create_table.return_value = created_table_mock

    tables_compat = bigquery_compat._ClientTablesCompat(client)
    request = mock.MagicMock()
    request.projectId = "test-project"
    request.datasetId = "test_dataset"
    request.table = mock.MagicMock()
    request.table.tableReference = mock.MagicMock()
    request.table.tableReference.projectId = "test-project"
    request.table.tableReference.datasetId = "test_dataset"
    request.table.tableReference.tableId = "test_table"
    request.table.schema = None
    request.table.labels = {"env": "test", "tier": "frontend"}
    request.table.friendlyName = "My Test Table"
    request.table.description = "A test table description"
    request.table.timePartitioning = None
    request.table.rangePartitioning = None
    request.table.clustering = None
    request.table.encryptionConfiguration = None

    tables_compat.Insert(request)
    client.create_table.assert_called_once()
    passed_table = client.create_table.call_args.args[0]
    self.assertEqual(passed_table.labels, {"env": "test", "tier": "frontend"})
    self.assertEqual(passed_table.friendly_name, "My Test Table")
    self.assertEqual(passed_table.description, "A test table description")


class TestTablePartitioningAndClusteringCompatibility(unittest.TestCase):
  def test_time_partitioning_from_dict_and_camel_case(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    table = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table.timePartitioning = {"type": "DAY"}
    self.assertIsNotNone(table.time_partitioning)
    self.assertEqual(table.time_partitioning.type_, "DAY")
    self.assertEqual(table.timePartitioning.type, "DAY")

    table2 = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table2.time_partitioning = {
        "type": "HOUR", "field": "ts", "expirationMs": 86400000
    }
    self.assertEqual(table2.time_partitioning.type_, "HOUR")
    self.assertEqual(table2.time_partitioning.field, "ts")
    self.assertEqual(table2.time_partitioning.expiration_ms, 86400000)

  def test_range_partitioning_from_dict_and_camel_case(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    table = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table.rangePartitioning = {
        "field": "id", "range": {
            "start": 0, "end": 100, "interval": 10
        }
    }
    self.assertIsNotNone(table.range_partitioning)
    self.assertEqual(table.range_partitioning.field, "id")
    self.assertEqual(table.range_partitioning.range_.start, 0)
    self.assertEqual(table.range_partitioning.range_.end, 100)
    self.assertEqual(table.range_partitioning.range_.interval, 10)

  def test_clustering_from_dict_and_list(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    table = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table.clustering = {"fields": ["language", "country"]}
    self.assertEqual(table.clustering_fields, ["language", "country"])
    self.assertEqual(table.clustering.fields, ["language", "country"])
    self.assertEqual(table.clustering["fields"], ["language", "country"])
    self.assertEqual(table.clustering.get("fields"), ["language", "country"])

    table2 = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table2.clustering = ["language"]
    self.assertEqual(table2.clustering_fields, ["language"])
    self.assertEqual(table2.clustering.fields, ["language"])

  def test_client_tables_compat_insert_with_partitioning_and_clustering(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.Mock()
    tables_compat = bigquery_compat._ClientTablesCompat(client)

    request = mock.Mock()
    request.projectId = "my-proj"
    request.datasetId = "my-ds"
    request.table = mock.Mock()
    request.table.tableReference = mock.Mock()
    request.table.tableReference.projectId = "my-proj"
    request.table.tableReference.datasetId = "my-ds"
    request.table.tableReference.tableId = "my-tbl"
    request.table.schema = None
    request.table.timePartitioning = {"type": "DAY"}
    request.table.rangePartitioning = None
    request.table.clustering = {"fields": ["language"]}
    request.table.description = None
    request.table.friendlyName = None
    request.table.labels = None
    request.table.encryptionConfiguration = None

    tables_compat.Insert(request)
    client.create_table.assert_called_once()
    passed_table = client.create_table.call_args.args[0]
    self.assertEqual(passed_table.time_partitioning.type_, "DAY")
    self.assertEqual(passed_table.clustering_fields, ["language"])

  def test_range_partitioning_from_apitools_with_zero_start(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    # If apitools RangePartitioning model is available, test with it directly
    if apitools_bigquery is not None and hasattr(apitools_bigquery,
                                                 "RangePartitioning"):
      rp = apitools_bigquery.RangePartitioning(
          field="id",
          range=apitools_bigquery.RangePartitioning.RangeValue(
              start=0, end=100, interval=10))
      gcp_rp = bigquery_compat._to_gcp_range_partitioning(rp)
      self.assertEqual(gcp_rp.field, "id")
      self.assertEqual(gcp_rp.range_.start, 0)
      self.assertEqual(gcp_rp.range_.end, 100)
      self.assertEqual(gcp_rp.range_.interval, 10)

    # Also test with object having start=0
    class RangeObj:
      def __init__(self):
        self.start = 0
        self.end = 50
        self.interval = 5

    class RPObj:
      def __init__(self):
        self.field = "num"
        self.range = RangeObj()

    gcp_rp2 = bigquery_compat._to_gcp_range_partitioning(RPObj())
    self.assertEqual(gcp_rp2.field, "num")
    self.assertEqual(gcp_rp2.range_.start, 0)
    self.assertEqual(gcp_rp2.range_.end, 50)
    self.assertEqual(gcp_rp2.range_.interval, 5)

  def test_time_partitioning_require_filter_false_and_str(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    # String input
    table = gcp_bigquery.Table("my-proj.my_ds.my_tbl")
    table.timePartitioning = "DAY"
    self.assertEqual(table.time_partitioning.type_, "DAY")

    # requirePartitionFilter = False and expirationMs = 0 explicitly preserved
    table2 = gcp_bigquery.Table("my-proj.my_ds.my_tbl2")
    table2.timePartitioning = {
        "type": "HOUR",
        "requirePartitionFilter": False,
        "expirationMs": 0,
    }
    self.assertEqual(table2.time_partitioning.type_, "HOUR")
    self.assertIs(table2.time_partitioning.require_partition_filter, False)
    self.assertEqual(table2.time_partitioning.expiration_ms, 0)

  def test_client_tables_compat_insert_with_bare_mock(self):
    if gcp_bigquery is None:
      raise unittest.SkipTest("google-cloud-bigquery is not installed")

    client = mock.Mock()
    tables_compat = bigquery_compat._ClientTablesCompat(client)

    # Bare mock where request.table has unconfigured attributes returning Mocks
    req = mock.Mock()
    req.table = mock.Mock()
    req.table.tableReference = mock.Mock()
    req.table.tableReference.projectId = "my-proj"
    req.table.tableReference.datasetId = "my-ds"
    req.table.tableReference.tableId = "my-tbl"
    req.table.schema = None

    tables_compat.Insert(req)
    client.create_table.assert_called_once()


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
