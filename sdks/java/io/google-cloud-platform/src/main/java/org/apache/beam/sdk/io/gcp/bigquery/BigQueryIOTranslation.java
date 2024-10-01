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

import static org.apache.beam.sdk.util.construction.TransformUpgrader.fromByteArray;
import static org.apache.beam.sdk.util.construction.TransformUpgrader.toByteArray;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest.MissingValueInterpretation;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.io.IOException;
import java.io.InvalidClassException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.FromBeamRowFunction;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.ToBeamRowFunction;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.SchemaUpdateOption;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.RowWriterFactory.AvroRowWriterFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.util.construction.TransformUpgrader;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"rawtypes", "nullness"})
public class BigQueryIOTranslation {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOTranslation.class);

  static class BigQueryIOReadTranslator implements TransformPayloadTranslator<TypedRead<?>> {

    static Schema schema =
        Schema.builder()
            .addNullableStringField("json_table_ref")
            .addNullableStringField("query")
            .addNullableBooleanField("validate")
            .addNullableBooleanField("flatten_results")
            .addNullableBooleanField("use_legacy_sql")
            .addNullableBooleanField("with_template_compatibility")
            .addNullableByteArrayField("bigquery_services")
            .addNullableByteArrayField("parse_fn")
            .addNullableByteArrayField("datum_reader_factory")
            .addNullableByteArrayField("query_priority")
            .addNullableStringField("query_location")
            .addNullableStringField("query_temp_dataset")
            .addNullableStringField("query_temp_project")
            .addNullableByteArrayField("method")
            .addNullableByteArrayField("format")
            .addNullableArrayField("selected_fields", FieldType.STRING)
            .addNullableStringField("row_restriction")
            .addNullableByteArrayField("coder")
            .addNullableStringField("kms_key")
            .addNullableByteArrayField("type_descriptor")
            .addNullableByteArrayField("to_beam_row_fn")
            .addNullableStringField("from_beam_row_fn")
            .addNullableBooleanField("use_avro_logical_types")
            .addNullableBooleanField("projection_pushdown_applied")
            .addNullableByteArrayField("bad_record_router")
            .addNullableByteArrayField("bad_record_error_handler")
            .build();

    public static final String BIGQUERY_READ_TRANSFORM_URN =
        "beam:transform:org.apache.beam:bigquery_read:v1";

    @Override
    public String getUrn() {
      return BIGQUERY_READ_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.@Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, TypedRead<?>> application, SdkComponents components)
        throws IOException {
      // Setting an empty payload since BigQuery transform payload is not actually used by runners
      // currently.
      // This can be implemented if runners started actually using the BigQuery transform payload.
      return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
    }

    @Override
    public Row toConfigRow(TypedRead<?> transform) {
      Map<String, Object> fieldValues = new HashMap<>();

      if (transform.getJsonTableRef() != null) {
        fieldValues.put("json_table_ref", transform.getJsonTableRef().get());
      }
      if (transform.getQuery() != null) {
        fieldValues.put("query", transform.getQuery().get());
      }
      fieldValues.put("validate", transform.getValidate());
      fieldValues.put("flatten_results", transform.getFlattenResults());
      fieldValues.put("use_legacy_sql", transform.getUseLegacySql());
      fieldValues.put("with_template_compatibility", transform.getWithTemplateCompatibility());

      if (transform.getBigQueryServices() != null) {
        fieldValues.put("bigquery_services", toByteArray(transform.getBigQueryServices()));
      }
      if (transform.getParseFn() != null) {
        fieldValues.put("parse_fn", toByteArray(transform.getParseFn()));
      }
      if (transform.getDatumReaderFactory() != null) {
        fieldValues.put("datum_reader_factory", toByteArray(transform.getDatumReaderFactory()));
      }
      if (transform.getQueryPriority() != null) {
        fieldValues.put("query_priority", toByteArray(transform.getQueryPriority()));
      }
      if (transform.getQueryLocation() != null) {
        fieldValues.put("query_location", transform.getQueryLocation());
      }
      if (transform.getQueryTempDataset() != null) {
        fieldValues.put("query_temp_dataset", transform.getQueryTempDataset());
      }
      if (transform.getQueryTempProject() != null) {
        fieldValues.put("query_temp_project", transform.getQueryTempProject());
      }
      if (transform.getMethod() != null) {
        fieldValues.put("method", toByteArray(transform.getMethod()));
      }
      if (transform.getFormat() != null) {
        fieldValues.put("format", toByteArray(transform.getFormat()));
      }
      if (transform.getSelectedFields() != null && !transform.getSelectedFields().get().isEmpty()) {
        fieldValues.put("selected_fields", transform.getSelectedFields().get());
      }
      if (transform.getRowRestriction() != null) {
        fieldValues.put("row_restriction", transform.getRowRestriction().get());
      }
      if (transform.getCoder() != null) {
        fieldValues.put("coder", toByteArray(transform.getCoder()));
      }
      if (transform.getKmsKey() != null) {
        fieldValues.put("kms_key", transform.getKmsKey());
      }
      if (transform.getTypeDescriptor() != null) {
        fieldValues.put("type_descriptor", toByteArray(transform.getTypeDescriptor()));
      }
      if (transform.getToBeamRowFn() != null) {
        fieldValues.put("to_beam_row_fn", toByteArray(transform.getToBeamRowFn()));
      }
      if (transform.getFromBeamRowFn() != null) {
        fieldValues.put("from_beam_row_fn", toByteArray(transform.getFromBeamRowFn()));
      }
      if (transform.getUseAvroLogicalTypes() != null) {
        fieldValues.put("use_avro_logical_types", transform.getUseAvroLogicalTypes());
      }
      fieldValues.put("projection_pushdown_applied", transform.getProjectionPushdownApplied());
      fieldValues.put("bad_record_router", toByteArray(transform.getBadRecordRouter()));
      fieldValues.put(
          "bad_record_error_handler", toByteArray(transform.getBadRecordErrorHandler()));

      return Row.withSchema(schema).withFieldValues(fieldValues).build();
    }

    @Override
    public TypedRead<?> fromConfigRow(Row configRow, PipelineOptions options) {
      String updateCompatibilityBeamVersion =
          options.as(StreamingOptions.class).getUpdateCompatibilityVersion();
      // We need to set a default 'updateCompatibilityBeamVersion' here since this PipelineOption
      // is not correctly passed in for pipelines that use Beam 2.53.0.
      // This is fixed for Beam 2.54.0 and later.
      updateCompatibilityBeamVersion =
          (updateCompatibilityBeamVersion != null) ? updateCompatibilityBeamVersion : "2.53.0";

      try {
        BigQueryIO.TypedRead.Builder builder = new AutoValue_BigQueryIO_TypedRead.Builder<>();

        String jsonTableRef = configRow.getString("json_table_ref");
        if (jsonTableRef != null) {
          builder = builder.setJsonTableRef(StaticValueProvider.of(jsonTableRef));
        }
        String query = configRow.getString("query");
        if (query != null) {
          builder = builder.setQuery(StaticValueProvider.of(query));
        }
        Boolean validate = configRow.getBoolean("validate");
        if (validate != null) {
          builder = builder.setValidate(validate);
        }
        Boolean flattenResults = configRow.getBoolean("flatten_results");
        if (flattenResults != null) {
          builder = builder.setFlattenResults(flattenResults);
        }
        Boolean useLegacySQL = configRow.getBoolean("use_legacy_sql");
        if (useLegacySQL != null) {
          builder = builder.setUseLegacySql(useLegacySQL);
        }
        Boolean withTemplateCompatibility = configRow.getBoolean("with_template_compatibility");
        if (withTemplateCompatibility != null) {
          builder = builder.setWithTemplateCompatibility(withTemplateCompatibility);
        }
        byte[] bigqueryServicesBytes = configRow.getBytes("bigquery_services");
        if (bigqueryServicesBytes != null) {
          try {
            builder =
                builder.setBigQueryServices(
                    (BigQueryServices) fromByteArray(bigqueryServicesBytes));
          } catch (InvalidClassException e) {
            LOG.warn(
                "Could not use the provided `BigQueryServices` implementation when upgrading."
                    + "Using the default.");
            builder.setBigQueryServices(new BigQueryServicesImpl());
          }
        }
        byte[] parseFnBytes = configRow.getBytes("parse_fn");
        if (parseFnBytes != null) {
          builder = builder.setParseFn((SerializableFunction) fromByteArray(parseFnBytes));
        }
        byte[] datumReaderFactoryBytes = configRow.getBytes("datum_reader_factory");
        if (datumReaderFactoryBytes != null) {
          builder =
              builder.setDatumReaderFactory(
                  (SerializableFunction) fromByteArray(datumReaderFactoryBytes));
        }
        byte[] queryPriorityBytes = configRow.getBytes("query_priority");
        if (queryPriorityBytes != null) {
          builder = builder.setQueryPriority((QueryPriority) fromByteArray(queryPriorityBytes));
        }
        String queryLocation = configRow.getString("query_location");
        if (queryLocation != null) {
          builder = builder.setQueryLocation(queryLocation);
        }
        String queryTempDataset = configRow.getString("query_temp_dataset");
        if (queryTempDataset != null) {
          builder = builder.setQueryTempDataset(queryTempDataset);
        }

        if (TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.57.0") >= 0) {
          // This property was added for Beam 2.57.0 hence not available when
          // upgrading the transform from previous Beam versions.
          String queryTempProject = configRow.getString("query_temp_project");
          if (queryTempProject != null) {
            builder = builder.setQueryTempProject(queryTempProject);
          }
        }

        byte[] methodBytes = configRow.getBytes("method");
        if (methodBytes != null) {
          builder = builder.setMethod((TypedRead.Method) fromByteArray(methodBytes));
        }
        byte[] formatBytes = configRow.getBytes("format");
        if (methodBytes != null) {
          builder = builder.setFormat((DataFormat) fromByteArray(formatBytes));
        }
        Collection<String> selectedFields = configRow.getArray("selected_fields");
        if (selectedFields != null && !selectedFields.isEmpty()) {
          builder.setSelectedFields(StaticValueProvider.of(ImmutableList.of(selectedFields)));
        }
        String rowRestriction = configRow.getString("row_restriction");
        if (rowRestriction != null) {
          builder = builder.setRowRestriction(StaticValueProvider.of(rowRestriction));
        }
        byte[] coderBytes = configRow.getBytes("coder");
        if (coderBytes != null) {
          try {
            builder = builder.setCoder((Coder) fromByteArray(coderBytes));
          } catch (InvalidClassException e) {
            LOG.warn(
                "Could not use the provided `Coder` implementation when upgrading."
                    + "Using the default.");
          }
        }
        String kmsKey = configRow.getString("kms_key");
        if (kmsKey != null) {
          builder = builder.setKmsKey(kmsKey);
        }
        byte[] typeDescriptorBytes = configRow.getBytes("type_descriptor");
        if (typeDescriptorBytes != null) {
          builder = builder.setTypeDescriptor((TypeDescriptor) fromByteArray(typeDescriptorBytes));
        }
        byte[] toBeamRowFnBytes = configRow.getBytes("to_beam_row_fn");
        if (toBeamRowFnBytes != null) {
          builder = builder.setToBeamRowFn((ToBeamRowFunction) fromByteArray(toBeamRowFnBytes));
        }
        byte[] fromBeamRowFnBytes = configRow.getBytes("from_beam_row_fn");
        if (fromBeamRowFnBytes != null) {
          builder =
              builder.setFromBeamRowFn((FromBeamRowFunction) fromByteArray(fromBeamRowFnBytes));
        }
        Boolean useAvroLogicalTypes = configRow.getBoolean("use_avro_logical_types");
        if (useAvroLogicalTypes != null) {
          builder = builder.setUseAvroLogicalTypes(useAvroLogicalTypes);
        }
        Boolean projectionPushdownApplied = configRow.getBoolean("projection_pushdown_applied");
        if (projectionPushdownApplied != null) {
          builder = builder.setProjectionPushdownApplied(projectionPushdownApplied);
        }

        if (TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.55.0") < 0) {
          // We need to use defaults here for BQ rear/write transforms upgraded
          // from older Beam versions.
          // See https://github.com/apache/beam/issues/30534.
          builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
          builder.setBadRecordErrorHandler(new BadRecordErrorHandler.DefaultErrorHandler<>());
        } else {
          byte[] badRecordRouter = configRow.getBytes("bad_record_router");
          builder.setBadRecordRouter((BadRecordRouter) fromByteArray(badRecordRouter));
          byte[] badRecordErrorHandler = configRow.getBytes("bad_record_error_handler");
          builder.setBadRecordErrorHandler(
              (ErrorHandler<BadRecord, ?>) fromByteArray(badRecordErrorHandler));
        }

        return builder.build();
      } catch (InvalidClassException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class ReadRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(AutoValue_BigQueryIO_TypedRead.class, new BigQueryIOReadTranslator())
          .build();
    }
  }

  static class BigQueryIOWriteTranslator implements TransformPayloadTranslator<Write<?>> {

    static Schema schema =
        Schema.builder()
            .addNullableStringField("json_table_ref")
            .addNullableByteArrayField("table_function")
            .addNullableByteArrayField("format_function")
            .addNullableByteArrayField("format_record_on_failure_function")
            .addNullableByteArrayField("avro_row_writer_factory")
            .addNullableByteArrayField("avro_schema_factory")
            .addNullableBooleanField("use_avro_logical_types")
            .addNullableByteArrayField("dynamic_destinations")
            .addNullableStringField("json_schema")
            .addNullableStringField("json_time_partitioning")
            .addNullableStringField("clustering")
            .addNullableByteArrayField("create_disposition")
            .addNullableByteArrayField("write_disposition")
            .addNullableArrayField("schema_update_options", FieldType.BYTES)
            .addNullableStringField("table_description")
            .addNullableBooleanField("validate")
            .addNullableByteArrayField("bigquery_services")
            .addNullableInt32Field("max_files_per_bundle")
            .addNullableInt64Field("max_file_size")
            .addNullableInt32Field("num_file_shards")
            .addNullableInt32Field("num_storage_write_api_streams")
            .addNullableBooleanField("propagate_successful_storage_api_writes")
            .addNullableByteArrayField("propagate_successful_storage_api_writes_predicate")
            .addNullableInt32Field("max_files_per_partition")
            .addNullableInt64Field("max_bytes_per_partition")
            .addNullableLogicalTypeField("triggering_frequency", new NanosDuration())
            .addNullableByteArrayField("method")
            .addNullableStringField("load_job_project_id")
            .addNullableByteArrayField("failed_insert_retry_policy")
            .addNullableStringField("custom_gcs_temp_location")
            .addNullableBooleanField("extended_error_info")
            .addNullableBooleanField("skip_invalid_rows")
            .addNullableBooleanField("ignore_unknown_values")
            .addNullableBooleanField("ignore_insert_ids")
            .addNullableInt32Field("max_retry_jobs")
            .addNullableStringField("kms_key")
            .addNullableArrayField("primary_key", FieldType.STRING)
            .addNullableByteArrayField("default_missing_value_interpretation")
            .addNullableBooleanField("optimize_writes")
            .addNullableBooleanField("use_beam_schema")
            .addNullableBooleanField("auto_sharding")
            .addNullableBooleanField("propagate_successful")
            .addNullableBooleanField("auto_schema_update")
            .addNullableByteArrayField("write_protos_class")
            .addNullableBooleanField("direct_write_protos")
            .addNullableByteArrayField("deterministic_record_id_fn")
            .addNullableStringField("write_temp_dataset")
            .addNullableByteArrayField("row_mutation_information_fn")
            .addNullableByteArrayField("bad_record_error_handler")
            .addNullableByteArrayField("bad_record_router")
            .build();

    public static final String BIGQUERY_WRITE_TRANSFORM_URN =
        "beam:transform:org.apache.beam:bigquery_write:v1";

    @Override
    public String getUrn() {
      return BIGQUERY_WRITE_TRANSFORM_URN;
    }

    @Override
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, Write<?>> application, SdkComponents components)
        throws IOException {
      // Setting an empty payload since BigQuery transform payload is not actually used by runners
      // currently.
      // This can be implemented if runners started actually using the Kafka BigQuery payload.
      return FunctionSpec.newBuilder().setUrn(getUrn()).setPayload(ByteString.empty()).build();
    }

    @Override
    public Row toConfigRow(Write<?> transform) {

      Map<String, Object> fieldValues = new HashMap<>();

      if (transform.getJsonTableRef() != null) {
        fieldValues.put("json_table_ref", transform.getJsonTableRef().get());
      }
      if (transform.getTableFunction() != null) {
        fieldValues.put("table_function", toByteArray(transform.getTableFunction()));
      }
      if (transform.getFormatFunction() != null) {
        fieldValues.put("format_function", toByteArray(transform.getFormatFunction()));
      }
      if (transform.getFormatRecordOnFailureFunction() != null) {
        fieldValues.put(
            "format_record_on_failure_function",
            toByteArray(transform.getFormatRecordOnFailureFunction()));
      }
      if (transform.getAvroRowWriterFactory() != null) {
        fieldValues.put(
            "avro_row_writer_factory", toByteArray(transform.getAvroRowWriterFactory()));
      }
      fieldValues.put("use_avro_logical_types", transform.getUseAvroLogicalTypes());
      if (transform.getDynamicDestinations() != null) {
        fieldValues.put("dynamic_destinations", toByteArray(transform.getDynamicDestinations()));
      }
      if (transform.getSchemaFromView() != null) {
        // Property 'getSchemaFromView' cannot be used in a portable way across pipelines since it
        // is bound to PCollections generated for the current pipeline instance.
        throw new IllegalArgumentException(
            "BigQueryIO.Write transforms cannot be converted to a "
                + "portable row based config due to 'withSchemaFromView' property being set. Please "
                + "retry without setting this property when configuring your transform");
      }
      if (transform.getJsonSchema() != null) {
        fieldValues.put("json_schema", transform.getJsonSchema().get());
      }
      if (transform.getJsonTimePartitioning() != null) {
        fieldValues.put(
            "json_time_partitioning", toByteArray(transform.getJsonTimePartitioning().get()));
      }
      if (transform.getJsonClustering() != null) {
        fieldValues.put("clustering", transform.getJsonClustering().get());
      }
      if (transform.getCreateDisposition() != null) {
        fieldValues.put("create_disposition", toByteArray(transform.getCreateDisposition()));
      }
      if (transform.getWriteDisposition() != null) {
        fieldValues.put("write_disposition", toByteArray(transform.getWriteDisposition()));
      }
      if (transform.getSchemaUpdateOptions() != null
          && !transform.getSchemaUpdateOptions().isEmpty()) {
        List<byte[]> schemUpdateOptionsData =
            transform.getSchemaUpdateOptions().stream()
                .map(option -> toByteArray(option))
                .collect(Collectors.toList());
        fieldValues.put("schema_update_options", schemUpdateOptionsData);
      }
      if (transform.getTableDescription() != null) {
        fieldValues.put("table_description", transform.getTableDescription());
      }
      fieldValues.put("validate", transform.getValidate());
      if (transform.getBigQueryServices() != null) {
        fieldValues.put("bigquery_services", toByteArray(transform.getBigQueryServices()));
      }
      if (transform.getMaxFilesPerBundle() != null) {
        fieldValues.put("max_files_per_bundle", transform.getMaxFilesPerBundle());
      }
      if (transform.getMaxFileSize() != null) {
        fieldValues.put("max_file_size", transform.getMaxFileSize());
      }
      fieldValues.put("num_file_shards", transform.getNumFileShards());
      fieldValues.put("num_storage_write_api_streams", transform.getNumStorageWriteApiStreams());
      fieldValues.put(
          "propagate_successful_storage_api_writes",
          transform.getPropagateSuccessfulStorageApiWrites());
      fieldValues.put(
          "propagate_successful_storage_api_writes_predicate",
          toByteArray(transform.getPropagateSuccessfulStorageApiWritesPredicate()));
      fieldValues.put("max_files_per_partition", transform.getMaxFilesPerPartition());
      fieldValues.put("max_bytes_per_partition", transform.getMaxBytesPerPartition());
      if (transform.getTriggeringFrequency() != null) {
        fieldValues.put(
            "triggering_frequency",
            Duration.ofMillis(transform.getTriggeringFrequency().getMillis()));
      }
      if (transform.getMethod() != null) {
        fieldValues.put("method", toByteArray(transform.getMethod()));
      }
      if (transform.getLoadJobProjectId() != null) {
        fieldValues.put("load_job_project_id", transform.getLoadJobProjectId());
      }
      if (transform.getFailedInsertRetryPolicy() != null) {
        fieldValues.put(
            "failed_insert_retry_policy", toByteArray(transform.getFailedInsertRetryPolicy()));
      }
      if (transform.getCustomGcsTempLocation() != null) {
        fieldValues.put("custom_gcs_temp_location", transform.getCustomGcsTempLocation().get());
      }
      fieldValues.put("extended_error_info", transform.getExtendedErrorInfo());
      if (transform.getSkipInvalidRows() != null) {
        fieldValues.put("skip_invalid_rows", transform.getSkipInvalidRows());
      }
      if (transform.getIgnoreUnknownValues() != null) {
        fieldValues.put("ignore_unknown_values", transform.getIgnoreUnknownValues());
      }
      if (transform.getIgnoreInsertIds() != null) {
        fieldValues.put("ignore_insert_ids", transform.getIgnoreInsertIds());
      }
      fieldValues.put("max_retry_jobs", transform.getMaxRetryJobs());
      if (transform.getPropagateSuccessful() != null) {
        fieldValues.put("propagate_successful", transform.getPropagateSuccessful());
      }
      if (transform.getKmsKey() != null) {
        fieldValues.put("kms_key", transform.getKmsKey());
      }
      if (transform.getPrimaryKey() != null) {
        fieldValues.put("primary_key", transform.getPrimaryKey());
      }
      if (transform.getDefaultMissingValueInterpretation() != null) {
        fieldValues.put(
            "default_missing_value_interpretation",
            toByteArray(transform.getDefaultMissingValueInterpretation()));
      }
      if (transform.getOptimizeWrites() != null) {
        fieldValues.put("optimize_writes", transform.getOptimizeWrites());
      }
      if (transform.getUseBeamSchema() != null) {
        fieldValues.put("use_beam_schema", transform.getUseBeamSchema());
      }
      if (transform.getAutoSharding() != null) {
        fieldValues.put("auto_sharding", transform.getAutoSharding());
      }
      if (transform.getAutoSchemaUpdate() != null) {
        fieldValues.put("auto_schema_update", transform.getAutoSchemaUpdate());
      }
      if (transform.getWriteProtosClass() != null) {
        fieldValues.put("write_protos_class", toByteArray(transform.getWriteProtosClass()));
      }
      if (transform.getDirectWriteProtos() != null) {
        fieldValues.put("direct_write_protos", transform.getDirectWriteProtos());
      }
      if (transform.getDeterministicRecordIdFn() != null) {
        fieldValues.put(
            "deterministic_record_id_fn", toByteArray(transform.getDeterministicRecordIdFn()));
      }
      if (transform.getWriteTempDataset() != null) {
        fieldValues.put("write_temp_dataset", toByteArray(transform.getWriteTempDataset()));
      }
      if (transform.getRowMutationInformationFn() != null) {
        fieldValues.put(
            "row_mutation_information_fn", toByteArray(transform.getRowMutationInformationFn()));
      }
      fieldValues.put("bad_record_router", toByteArray(transform.getBadRecordRouter()));
      fieldValues.put(
          "bad_record_error_handler", toByteArray(transform.getBadRecordErrorHandler()));

      return Row.withSchema(schema).withFieldValues(fieldValues).build();
    }

    @Override
    public Write<?> fromConfigRow(Row configRow, PipelineOptions options) {
      String updateCompatibilityBeamVersion =
          options.as(StreamingOptions.class).getUpdateCompatibilityVersion();
      // We need to set a default 'updateCompatibilityBeamVersion' here since this PipelineOption
      // is not correctly passed in for pipelines that use Beam 2.53.0.
      // This is fixed for Beam 2.54.0 and later.
      updateCompatibilityBeamVersion =
          (updateCompatibilityBeamVersion != null) ? updateCompatibilityBeamVersion : "2.53.0";

      try {
        BigQueryIO.Write.Builder builder = new AutoValue_BigQueryIO_Write.Builder<>();

        String jsonTableRef = configRow.getString("json_table_ref");
        if (jsonTableRef != null) {
          builder = builder.setJsonTableRef(StaticValueProvider.of(jsonTableRef));
        }
        byte[] tableFunctionBytes = configRow.getBytes("table_function");
        if (tableFunctionBytes != null) {
          builder =
              builder.setTableFunction(
                  (SerializableFunction<ValueInSingleWindow, TableDestination>)
                      fromByteArray(tableFunctionBytes));
        }
        byte[] formatFunctionBytes = configRow.getBytes("format_function");
        if (formatFunctionBytes != null) {
          builder =
              builder.setFormatFunction(
                  (SerializableFunction<?, TableRow>) fromByteArray(formatFunctionBytes));
        }
        byte[] formatRecordOnFailureFunctionBytes =
            configRow.getBytes("format_record_on_failure_function");
        if (tableFunctionBytes != null) {
          builder =
              builder.setFormatRecordOnFailureFunction(
                  (SerializableFunction<?, TableRow>)
                      fromByteArray(formatRecordOnFailureFunctionBytes));
        }
        byte[] avroRowWriterFactoryBytes = configRow.getBytes("avro_row_writer_factory");
        if (avroRowWriterFactoryBytes != null) {
          builder =
              builder.setAvroRowWriterFactory(
                  (AvroRowWriterFactory) fromByteArray(avroRowWriterFactoryBytes));
        }
        byte[] avroSchemaFactoryBytes = configRow.getBytes("avro_schema_factory");
        if (tableFunctionBytes != null) {
          builder =
              builder.setAvroSchemaFactory(
                  (SerializableFunction) fromByteArray(avroSchemaFactoryBytes));
        }
        Boolean useAvroLogicalTypes = configRow.getBoolean("use_avro_logical_types");
        if (useAvroLogicalTypes != null) {
          builder = builder.setUseAvroLogicalTypes(useAvroLogicalTypes);
        }
        byte[] dynamicDestinationsBytes = configRow.getBytes("dynamic_destinations");
        if (dynamicDestinationsBytes != null) {
          builder =
              builder.setDynamicDestinations(
                  (DynamicDestinations) fromByteArray(dynamicDestinationsBytes));
        }
        String jsonSchema = configRow.getString("json_schema");
        if (jsonSchema != null) {
          builder = builder.setJsonSchema(StaticValueProvider.of(jsonSchema));
        }
        String jsonTimePartitioning = configRow.getString("json_time_partitioning");
        if (jsonTimePartitioning != null) {
          builder = builder.setJsonTimePartitioning(StaticValueProvider.of(jsonTimePartitioning));
        }
        // Translation with Clustering is broken before 2.56.0, where we used to attempt to
        // serialize a non-serializable Clustering object to bytes.
        // In 2.56.0 onwards, we translate using the json string representation instead.
        if (TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.56.0") >= 0) {
          String jsonClustering = configRow.getString("clustering");
          if (jsonClustering != null) {
            builder = builder.setJsonClustering(StaticValueProvider.of(jsonClustering));
          }
        }
        byte[] createDispositionBytes = configRow.getBytes("create_disposition");
        if (createDispositionBytes != null) {
          builder =
              builder.setCreateDisposition(
                  (CreateDisposition) fromByteArray(createDispositionBytes));
        }
        byte[] writeDispositionBytes = configRow.getBytes("write_disposition");
        if (writeDispositionBytes != null) {
          builder =
              builder.setWriteDisposition((WriteDisposition) fromByteArray(writeDispositionBytes));
        }
        Collection<byte[]> schemaUpdateOptionsData = configRow.getArray("schema_update_options");
        if (schemaUpdateOptionsData != null) {
          Set<SchemaUpdateOption> schemaUpdateOptions =
              schemaUpdateOptionsData.stream()
                  .map(
                      data -> {
                        try {
                          return (SchemaUpdateOption) fromByteArray(data);
                        } catch (InvalidClassException e) {
                          throw new RuntimeException(e);
                        }
                      })
                  .collect(Collectors.toSet());
          builder = builder.setSchemaUpdateOptions(schemaUpdateOptions);
        } else {
          // This property is not nullable.
          builder = builder.setSchemaUpdateOptions(Collections.emptySet());
        }
        String tableDescription = configRow.getString("table_description");
        if (tableDescription != null) {
          builder = builder.setTableDescription(tableDescription);
        }
        Boolean validate = configRow.getBoolean("validate");
        if (validate != null) {
          builder = builder.setValidate(validate);
        }
        byte[] bigqueryServicesBytes = configRow.getBytes("bigquery_services");
        if (bigqueryServicesBytes != null) {
          try {
            builder =
                builder.setBigQueryServices(
                    (BigQueryServices) fromByteArray(bigqueryServicesBytes));
          } catch (InvalidClassException e) {
            LOG.warn(
                "Could not use the provided `BigQueryServices` implementation when upgrading."
                    + "Using the default.");
            builder.setBigQueryServices(new BigQueryServicesImpl());
          }
        }
        Integer maxFilesPerBundle = configRow.getInt32("max_files_per_bundle");
        if (maxFilesPerBundle != null) {
          builder = builder.setMaxFilesPerBundle(maxFilesPerBundle);
        }
        Long maxFileSize = configRow.getInt64("max_file_size");
        if (maxFileSize != null) {
          builder = builder.setMaxFileSize(maxFileSize);
        }
        Integer numFileShards = configRow.getInt32("num_file_shards");
        if (numFileShards != null) {
          builder = builder.setNumFileShards(numFileShards);
        }
        Integer numStorageWriteApiStreams = configRow.getInt32("num_storage_write_api_streams");
        if (numStorageWriteApiStreams != null) {
          builder = builder.setNumStorageWriteApiStreams(numStorageWriteApiStreams);
        }

        if (TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.60.0") >= 0) {
          Boolean propagateSuccessfulStorageApiWrites =
              configRow.getBoolean("propagate_successful_storage_api_writes");
          if (propagateSuccessfulStorageApiWrites != null) {
            builder =
                builder.setPropagateSuccessfulStorageApiWrites(propagateSuccessfulStorageApiWrites);
          }

          byte[] predicate =
              configRow.getBytes("propagate_successful_storage_api_writes_predicate");
          if (predicate != null) {
            builder =
                builder.setPropagateSuccessfulStorageApiWritesPredicate(
                    (Predicate<String>) fromByteArray(predicate));
          }
        } else {
          builder.setPropagateSuccessfulStorageApiWrites(false);
          builder.setPropagateSuccessfulStorageApiWritesPredicate(Predicates.alwaysTrue());
        }

        Integer maxFilesPerPartition = configRow.getInt32("max_files_per_partition");
        if (maxFilesPerPartition != null) {
          builder = builder.setMaxFilesPerPartition(maxFilesPerPartition);
        }
        Long maxBytesPerPartition = configRow.getInt64("max_bytes_per_partition");
        if (maxBytesPerPartition != null) {
          builder = builder.setMaxBytesPerPartition(maxBytesPerPartition);
        }

        // We need to update the 'triggerring_frequency' field name for pipelines that are upgraded
        // from Beam 2.53.0 due to https://github.com/apache/beam/pull/29785.
        // This is fixed for Beam 2.54.0 and later.
        String triggeringFrequencyFieldName =
            TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.53.0") == 0
                ? "triggerring_frequency"
                : "triggering_frequency";

        Duration triggeringFrequency = configRow.getValue(triggeringFrequencyFieldName);
        if (triggeringFrequency != null) {
          builder =
              builder.setTriggeringFrequency(
                  org.joda.time.Duration.millis(triggeringFrequency.toMillis()));
        }
        byte[] methodBytes = configRow.getBytes("method");
        if (methodBytes != null) {
          builder = builder.setMethod((Write.Method) fromByteArray(methodBytes));
        }
        String loadJobProjectId = configRow.getString("load_job_project_id");
        if (loadJobProjectId != null) {
          builder = builder.setLoadJobProjectId(StaticValueProvider.of(loadJobProjectId));
        }
        byte[] failedInsertRetryPolicyBytes = configRow.getBytes("failed_insert_retry_policy");
        if (failedInsertRetryPolicyBytes != null) {
          builder =
              builder.setFailedInsertRetryPolicy(
                  (InsertRetryPolicy) fromByteArray(failedInsertRetryPolicyBytes));
        }
        String customGcsTempLocations = configRow.getString("custom_gcs_temp_location");
        if (customGcsTempLocations != null) {
          builder =
              builder.setCustomGcsTempLocation(StaticValueProvider.of(customGcsTempLocations));
        }
        Boolean extendedErrorInfo = configRow.getBoolean("extended_error_info");
        if (extendedErrorInfo != null) {
          builder = builder.setExtendedErrorInfo(extendedErrorInfo);
        }
        Boolean skipInvalidRows = configRow.getBoolean("skip_invalid_rows");
        if (skipInvalidRows != null) {
          builder = builder.setSkipInvalidRows(skipInvalidRows);
        }
        Boolean ignoreUnknownValues = configRow.getBoolean("ignore_unknown_values");
        if (ignoreUnknownValues != null) {
          builder = builder.setIgnoreUnknownValues(ignoreUnknownValues);
        }
        Boolean ignoreInsertIds = configRow.getBoolean("ignore_insert_ids");
        if (ignoreInsertIds != null) {
          builder = builder.setIgnoreInsertIds(ignoreInsertIds);
        }
        Integer maxRetryJobs = configRow.getInt32("max_retry_jobs");
        if (maxRetryJobs != null) {
          builder = builder.setMaxRetryJobs(maxRetryJobs);
        }
        String kmsKey = configRow.getString("kms_key");
        if (kmsKey != null) {
          builder = builder.setKmsKey(kmsKey);
        }
        Collection<String> primaryKey = configRow.getArray("primary_key");
        if (primaryKey != null && !primaryKey.isEmpty()) {
          builder = builder.setPrimaryKey(ImmutableList.of(primaryKey));
        }
        byte[] defaultMissingValueInterpretationsBytes =
            configRow.getBytes("default_missing_value_interpretation");
        if (defaultMissingValueInterpretationsBytes != null) {
          builder =
              builder.setDefaultMissingValueInterpretation(
                  (MissingValueInterpretation)
                      fromByteArray(defaultMissingValueInterpretationsBytes));
        }
        Boolean optimizeWrites = configRow.getBoolean("optimize_writes");
        if (optimizeWrites != null) {
          builder = builder.setOptimizeWrites(optimizeWrites);
        }
        Boolean useBeamSchema = configRow.getBoolean("use_beam_schema");
        if (useBeamSchema != null) {
          builder = builder.setUseBeamSchema(useBeamSchema);
        }
        Boolean autoSharding = configRow.getBoolean("auto_sharding");
        if (autoSharding != null) {
          builder = builder.setAutoSharding(autoSharding);
        }
        Boolean propagateSuccessful = configRow.getBoolean("propagate_successful");
        if (propagateSuccessful != null) {
          builder = builder.setPropagateSuccessful(propagateSuccessful);
        }
        Boolean autoSchemaUpdate = configRow.getBoolean("auto_schema_update");
        if (autoSchemaUpdate != null) {
          builder = builder.setAutoSchemaUpdate(autoSchemaUpdate);
        }
        byte[] writeProtosClasses = configRow.getBytes("write_protos_class");
        if (writeProtosClasses != null) {
          builder =
              builder.setWriteProtosClass(
                  (Class) fromByteArray(defaultMissingValueInterpretationsBytes));
        }
        Boolean directWriteProtos = configRow.getBoolean("direct_write_protos");
        if (directWriteProtos != null) {
          builder = builder.setDirectWriteProtos(directWriteProtos);
        }
        byte[] deterministicRecordIdFnBytes = configRow.getBytes("deterministic_record_id_fn");
        if (deterministicRecordIdFnBytes != null) {
          builder =
              builder.setDeterministicRecordIdFn(
                  (SerializableFunction) fromByteArray(deterministicRecordIdFnBytes));
        }
        String writeTempDataset = configRow.getString("write_temp_dataset");
        if (writeTempDataset != null) {
          builder = builder.setWriteTempDataset(writeTempDataset);
        }
        byte[] rowMutationInformationFnBytes = configRow.getBytes("row_mutation_information_fn");
        if (rowMutationInformationFnBytes != null) {
          builder =
              builder.setRowMutationInformationFn(
                  (SerializableFunction) fromByteArray(rowMutationInformationFnBytes));
        }

        if (TransformUpgrader.compareVersions(updateCompatibilityBeamVersion, "2.55.0") < 0) {
          // We need to use defaults here for BQ rear/write transforms upgraded
          // from older Beam versions.
          // See https://github.com/apache/beam/issues/30534.
          builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
          builder.setBadRecordErrorHandler(new BadRecordErrorHandler.DefaultErrorHandler<>());
        } else {
          byte[] badRecordRouter = configRow.getBytes("bad_record_router");
          builder.setBadRecordRouter((BadRecordRouter) fromByteArray(badRecordRouter));
          byte[] badRecordErrorHandler = configRow.getBytes("bad_record_error_handler");
          builder.setBadRecordErrorHandler(
              (ErrorHandler<BadRecord, ?>) fromByteArray(badRecordErrorHandler));
        }

        return builder.build();
      } catch (InvalidClassException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class WriteRegistrar implements TransformPayloadTranslatorRegistrar {

    @Override
    @SuppressWarnings({
      "rawtypes",
    })
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.<Class<? extends PTransform>, TransformPayloadTranslator>builder()
          .put(AutoValue_BigQueryIO_Write.class, new BigQueryIOWriteTranslator())
          .build();
    }
  }
}
