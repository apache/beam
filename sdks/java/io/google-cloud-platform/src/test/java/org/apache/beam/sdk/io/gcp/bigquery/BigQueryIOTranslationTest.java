/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

public class BigQueryIOTranslationTest {

  // A mapping from Read transform builder methods to the corresponding schema fields in
  // BigQueryIOTranslation.
  static final Map<String, String> READ_TRANSFORM_SCHEMA_MAPPING = new HashMap<>();

  static {
    READ_TRANSFORM_SCHEMA_MAPPING.put("getJsonTableRef", "json_table_ref");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getQuery", "query");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getValidate", "validate");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getFlattenResults", "flatten_results");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getUseLegacySql", "use_legacy_sql");
    READ_TRANSFORM_SCHEMA_MAPPING.put(
        "getWithTemplateCompatibility", "with_template_compatibility");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getBigQueryServices", "bigquery_services");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getParseFn", "parse_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getDatumReaderFactory", "datum_reader_factory");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getQueryPriority", "query_priority");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getQueryLocation", "query_location");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getQueryTempDataset", "query_temp_dataset");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getQueryTempProject", "query_temp_project");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getMethod", "method");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getFormat", "format");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getSelectedFields", "selected_fields");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getRowRestriction", "row_restriction");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getCoder", "coder");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getKmsKey", "kms_key");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getTypeDescriptor", "type_descriptor");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getToBeamRowFn", "to_beam_row_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getFromBeamRowFn", "from_beam_row_fn");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getUseAvroLogicalTypes", "use_avro_logical_types");
    READ_TRANSFORM_SCHEMA_MAPPING.put(
        "getProjectionPushdownApplied", "projection_pushdown_applied");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getBadRecordRouter", "bad_record_router");
    READ_TRANSFORM_SCHEMA_MAPPING.put("getBadRecordErrorHandler", "bad_record_error_handler");
  }

  static final Map<String, String> WRITE_TRANSFORM_SCHEMA_MAPPING = new HashMap<>();

  static {
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getJsonTableRef", "json_table_ref");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getTableFunction", "table_function");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getFormatFunction", "format_function");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getFormatRecordOnFailureFunction", "format_record_on_failure_function");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getAvroRowWriterFactory", "avro_row_writer_factory");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getAvroSchemaFactory", "avro_schema_factory");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getUseAvroLogicalTypes", "use_avro_logical_types");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getDynamicDestinations", "dynamic_destinations");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getJsonSchema", "json_schema");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getJsonTimePartitioning", "json_time_partitioning");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getJsonClustering", "clustering");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getCreateDisposition", "create_disposition");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getWriteDisposition", "write_disposition");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getSchemaUpdateOptions", "schema_update_options");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getTableDescription", "table_description");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getValidate", "validate");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getBigQueryServices", "bigquery_services");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMaxFilesPerBundle", "max_files_per_bundle");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMaxFileSize", "max_file_size");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getNumFileShards", "num_file_shards");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getNumStorageWriteApiStreams", "num_storage_write_api_streams");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getPropagateSuccessfulStorageApiWrites", "propagate_successful_storage_api_writes");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getPropagateSuccessfulStorageApiWritesPredicate",
        "propagate_successful_storage_api_writes_predicate");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMaxFilesPerPartition", "max_files_per_partition");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMaxBytesPerPartition", "max_bytes_per_partition");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getTriggeringFrequency", "triggering_frequency");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMethod", "method");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getLoadJobProjectId", "load_job_project_id");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getFailedInsertRetryPolicy", "failed_insert_retry_policy");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getCustomGcsTempLocation", "custom_gcs_temp_location");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getExtendedErrorInfo", "extended_error_info");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getSkipInvalidRows", "skip_invalid_rows");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getIgnoreUnknownValues", "ignore_unknown_values");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getIgnoreInsertIds", "ignore_insert_ids");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getMaxRetryJobs", "max_retry_jobs");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getKmsKey", "kms_key");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getPrimaryKey", "primary_key");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getDefaultMissingValueInterpretation", "default_missing_value_interpretation");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getOptimizeWrites", "optimize_writes");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getUseBeamSchema", "use_beam_schema");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getAutoSharding", "auto_sharding");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getPropagateSuccessful", "propagate_successful");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getAutoSchemaUpdate", "auto_schema_update");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getWriteProtosClass", "write_protos_class");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getDirectWriteProtos", "direct_write_protos");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getDeterministicRecordIdFn", "deterministic_record_id_fn");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getWriteTempDataset", "write_temp_dataset");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put(
        "getRowMutationInformationFn", "row_mutation_information_fn");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getBadRecordRouter", "bad_record_router");
    WRITE_TRANSFORM_SCHEMA_MAPPING.put("getBadRecordErrorHandler", "bad_record_error_handler");
  }

  @Test
  public void testReCreateReadTransformFromRowTable() {
    // setting a subset of fields here.
    BigQueryIO.TypedRead<TableRow> readTransform =
        BigQueryIO.readTableRows()
            .from("dummyproject:dummydataset.dummytable")
            .withMethod(TypedRead.Method.DIRECT_READ)
            .withKmsKey("dummykmskey")
            .withTemplateCompatibility();

    BigQueryIOTranslation.BigQueryIOReadTranslator translator =
        new BigQueryIOTranslation.BigQueryIOReadTranslator();
    Row row = translator.toConfigRow(readTransform);

    BigQueryIO.TypedRead<TableRow> readTransformFromRow =
        (BigQueryIO.TypedRead<TableRow>)
            translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(readTransformFromRow.getTable());
    assertEquals("dummyproject", readTransformFromRow.getTable().getProjectId());
    assertEquals("dummydataset", readTransformFromRow.getTable().getDatasetId());
    assertEquals("dummytable", readTransformFromRow.getTable().getTableId());
    assertEquals(TypedRead.Method.DIRECT_READ, readTransformFromRow.getMethod());
    assertEquals("dummykmskey", readTransformFromRow.getKmsKey());
    assertTrue(readTransformFromRow.getWithTemplateCompatibility());
  }

  static class DummyParseFn implements SerializableFunction<SchemaAndRecord, Object> {
    @Override
    public Object apply(SchemaAndRecord input) {
      return null;
    }
  }

  @Test
  public void testReCreateReadTransformFromRowQuery() {
    // setting a subset of fields here.
    BigQueryIO.TypedRead<?> readTransform =
        BigQueryIO.read(new DummyParseFn())
            .fromQuery("dummyquery")
            .useAvroLogicalTypes()
            .usingStandardSql();

    BigQueryIOTranslation.BigQueryIOReadTranslator translator =
        new BigQueryIOTranslation.BigQueryIOReadTranslator();
    Row row = translator.toConfigRow(readTransform);

    BigQueryIO.TypedRead<?> readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertEquals("dummyquery", readTransformFromRow.getQuery().get());
    assertNotNull(readTransformFromRow.getParseFn());
    assertTrue(readTransformFromRow.getParseFn() instanceof DummyParseFn);
    assertTrue(readTransformFromRow.getUseAvroLogicalTypes());
    assertFalse(readTransformFromRow.getUseLegacySql());
  }

  @Test
  public void testReadTransformRowIncludesAllFields() {
    // These fields do not represent properties of the transform.
    List<String> fieldsToIgnore =
        ImmutableList.of("getFinalSchema", "getTableProvider", "getTable");

    List<String> getMethodNames =
        Arrays.stream(BigQueryIO.TypedRead.class.getDeclaredMethods())
            .map(
                method -> {
                  return method.getName();
                })
            .filter(methodName -> methodName.startsWith("get"))
            .filter(methodName -> !fieldsToIgnore.contains(methodName))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'BigQueryIO.TypedRead' transform. Please update"
              + "'BigQueryIOTranslation.BigQueryIOReadTranslator' to track the new method "
              + "and update this test.",
          READ_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `READ_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    READ_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "BigQueryIOTranslation.BigQueryIOReadTranslator.",
                  BigQueryIOTranslation.BigQueryIOReadTranslator.schema
                      .getFieldNames()
                      .contains(fieldName));
            });
  }

  @Test
  public void testReCreateWriteTransformFromRowTable() {
    // setting a subset of fields here.
    Clustering testClustering = new Clustering().setFields(Arrays.asList("a", "b", "c"));
    BigQueryIO.Write<?> writeTransform =
        BigQueryIO.write()
            .to("dummyproject:dummydataset.dummytable")
            .withAutoSharding()
            .withTriggeringFrequency(org.joda.time.Duration.millis(10000))
            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
            .withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withClustering(testClustering)
            .withKmsKey("dummykmskey");

    BigQueryIOTranslation.BigQueryIOWriteTranslator translator =
        new BigQueryIOTranslation.BigQueryIOWriteTranslator();
    Row row = translator.toConfigRow(writeTransform);

    PipelineOptions options = PipelineOptionsFactory.create();
    options.as(StreamingOptions.class).setUpdateCompatibilityVersion("2.56.0");
    BigQueryIO.Write<?> writeTransformFromRow =
        (BigQueryIO.Write<?>) translator.fromConfigRow(row, options);
    assertNotNull(writeTransformFromRow.getTable());
    assertEquals("dummyproject", writeTransformFromRow.getTable().get().getProjectId());
    assertEquals("dummydataset", writeTransformFromRow.getTable().get().getDatasetId());
    assertEquals("dummytable", writeTransformFromRow.getTable().get().getTableId());
    assertEquals(WriteDisposition.WRITE_TRUNCATE, writeTransformFromRow.getWriteDisposition());
    assertEquals(CreateDisposition.CREATE_NEVER, writeTransformFromRow.getCreateDisposition());
    assertEquals("dummykmskey", writeTransformFromRow.getKmsKey());
    assertEquals(
        BigQueryHelpers.toJsonString(testClustering),
        writeTransformFromRow.getJsonClustering().get());
  }

  @Test
  public void testWriteTransformRowIncludesAllFields() {
    // These fields do not represent properties of the transform.
    List<String> fieldsToIgnore =
        ImmutableList.of(
            "getSchemaFromView",
            "getStorageApiNumStreams",
            "getStorageApiTriggeringFrequency",
            "getTableWithDefaultProject",
            "getTable");

    List<String> getMethodNames =
        Arrays.stream(BigQueryIO.Write.class.getDeclaredMethods())
            .map(
                method -> {
                  return method.getName();
                })
            .filter(methodName -> methodName.startsWith("get"))
            .filter(methodName -> !fieldsToIgnore.contains(methodName))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'BigQueryIO.Write' transform. Please update"
              + "'BigQueryIOTranslation.BigQueryIOWriteTranslator' to track the new method "
              + "and update this test.",
          WRITE_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    WRITE_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "BigQueryIOTranslation.BigQueryIOWriteTranslator.",
                  BigQueryIOTranslation.BigQueryIOWriteTranslator.schema
                      .getFieldNames()
                      .contains(fieldName));
            });
  }
}
