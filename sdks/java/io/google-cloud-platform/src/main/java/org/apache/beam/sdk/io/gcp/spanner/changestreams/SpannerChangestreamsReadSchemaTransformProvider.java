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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.ReadSpannerSchema;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.Gson;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class SpannerChangestreamsReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        SpannerChangestreamsReadSchemaTransformProvider.SpannerChangestreamsReadConfiguration> {
  @Override
  protected Class<SpannerChangestreamsReadConfiguration> configurationClass() {
    return SpannerChangestreamsReadConfiguration.class;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangestreamsReadSchemaTransformProvider.class);

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableStringField("row").build();

  @Override
  public SchemaTransform from(
      SpannerChangestreamsReadSchemaTransformProvider.SpannerChangestreamsReadConfiguration
          configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Pipeline p = input.getPipeline();
        // TODO(pabloem): Does this action create/destroy a new metadata table??
        Schema tableChangesSchema = getTableSchema(configuration);
        SpannerIO.ReadChangeStream readChangeStream =
            SpannerIO.readChangeStream()
                .withSpannerConfig(
                    SpannerConfig.create()
                        .withProjectId(configuration.getProjectId())
                        .withInstanceId(configuration.getInstanceId())
                        .withDatabaseId(configuration.getDatabaseId()))
                .withChangeStreamName(configuration.getChangeStreamName())
                .withInclusiveStartAt(Timestamp.parseTimestamp(configuration.getStartAtTimestamp()))
                .withDatabaseId(configuration.getDatabaseId())
                .withProjectId(configuration.getProjectId())
                .withInstanceId(configuration.getInstanceId());

        if (configuration.getEndAtTimestamp() != null) {
          String endTs =
              Objects.requireNonNull(Objects.requireNonNull(configuration.getEndAtTimestamp()));
          readChangeStream = readChangeStream.withInclusiveEndAt(Timestamp.parseTimestamp(endTs));
        }

        PCollectionTuple outputTuple =
            p.apply(readChangeStream)
                .apply(
                    ParDo.of(
                            new DataChangeRecordToRow(
                                configuration.getTable(),
                                tableChangesSchema,
                                "SpannerChangestreams-read-error-counter"))
                        .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

        return PCollectionRowTuple.of(
            "output",
            outputTuple.get(OUTPUT_TAG).setRowSchema(tableChangesSchema),
            "errors",
            outputTuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
      }
    };
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:spanner_cdc_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output", "errors");
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class SpannerChangestreamsReadConfiguration implements Serializable {

    @SchemaFieldDescription("Specifies the Cloud Spanner database.")
    public abstract String getDatabaseId();

    @SchemaFieldDescription("Specifies the Cloud Spanner project.")
    public abstract String getProjectId();

    @SchemaFieldDescription("Specifies the Cloud Spanner instance.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Specifies the Cloud Spanner table.")
    public abstract String getTable();

    @SchemaFieldDescription("Specifies the time that the change stream should be read from.")
    public abstract String getStartAtTimestamp();

    @SchemaFieldDescription("Specifies the end time of the change stream.")
    public abstract @Nullable String getEndAtTimestamp();

    @SchemaFieldDescription("Specifies the change stream name.")
    public abstract String getChangeStreamName();

    public static Builder builder() {
      return new AutoValue_SpannerChangestreamsReadSchemaTransformProvider_SpannerChangestreamsReadConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDatabaseId(String databaseId);

      public abstract Builder setProjectId(String projectId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setTable(String table);

      public abstract Builder setStartAtTimestamp(String isoTimestamp);

      public abstract Builder setEndAtTimestamp(String isoTimestamp);

      public abstract Builder setChangeStreamName(String changeStreamName);

      public abstract SpannerChangestreamsReadConfiguration build();
    }
  }

  @VisibleForTesting
  public static final class DataChangeRecordToRow extends DoFn<DataChangeRecord, Row> {
    private final Schema tableChangeRecordSchema;
    private final String tableName;
    private transient Gson gson;
    private Counter errorCounter;
    private Long errorsInBundle = 0L;

    DataChangeRecordToRow(String tableName, Schema tableChangeRecordSchema, String name) {
      this.tableName = tableName;
      this.tableChangeRecordSchema = tableChangeRecordSchema;
      this.gson = new Gson();
      this.errorCounter =
          Metrics.counter(SpannerChangestreamsReadSchemaTransformProvider.class, name);
    }

    public Gson getGson() {
      if (gson == null) {
        gson = new Gson();
      }
      return gson;
    }

    @ProcessElement
    public void process(@DoFn.Element DataChangeRecord record, MultiOutputReceiver receiver) {
      if (!record.getTableName().equalsIgnoreCase(tableName)) {
        // If the element does not belong to the appropriate table name, we discard it.
        return;
      }
      final Instant timestamp = new Instant(record.getRecordTimestamp().toSqlTimestamp());

      for (Mod mod : record.getMods()) {
        Schema internalRowSchema =
            tableChangeRecordSchema.getField("rowValues").getType().getRowSchema();
        if (internalRowSchema == null) {
          throw new RuntimeException("Row schema for internal row is null and cannot be utilized.");
        }

        try {
          Row.FieldValueBuilder rowBuilder = Row.fromRow(Row.nullRow(internalRowSchema));
          final Map<String, String> newValues =
              Optional.ofNullable(mod.getNewValuesJson())
                  .map(nonNullValues -> getGson().fromJson(nonNullValues, Map.class))
                  .orElseGet(Collections::emptyMap);
          final Map<String, String> keyValues =
              Optional.ofNullable(mod.getKeysJson())
                  .map(nonNullValues -> getGson().fromJson(nonNullValues, Map.class))
                  .orElseGet(Collections::emptyMap);

          for (Map.Entry<String, String> valueEntry : newValues.entrySet()) {
            if (valueEntry.getValue() == null) {
              continue;
            }
            // TODO(pabloem): Understand why SpannerSchema has field names in lowercase...
            rowBuilder =
                rowBuilder.withFieldValue(
                    valueEntry.getKey().toLowerCase(),
                    stringToParsedValue(
                        internalRowSchema.getField(valueEntry.getKey().toLowerCase()).getType(),
                        valueEntry.getValue()));
          }

          for (Map.Entry<String, String> pkEntry : keyValues.entrySet()) {
            if (pkEntry.getValue() == null) {
              continue;
            }
            // TODO(pabloem): Understand why SpannerSchema has field names in lowercase...
            rowBuilder =
                rowBuilder.withFieldValue(
                    pkEntry.getKey().toLowerCase(),
                    stringToParsedValue(
                        internalRowSchema.getField(pkEntry.getKey().toLowerCase()).getType(),
                        pkEntry.getValue()));
          }
          receiver
              .get(OUTPUT_TAG)
              .outputWithTimestamp(
                  Row.withSchema(tableChangeRecordSchema)
                      .addValue(record.getModType().toString())
                      .addValue(record.getCommitTimestamp().toString())
                      .addValue(Long.parseLong(record.getRecordSequence()))
                      .addValue(rowBuilder.build())
                      .build(),
                  timestamp);
        } catch (Exception e) {
          errorsInBundle += 1;
          LOG.warn("Error while parsing the DataChangeRecord", e);
          String recordString = "Key:" + mod.getKeysJson() + " Value:" + mod.getNewValuesJson();
          receiver
              .get(ERROR_TAG)
              .output(Row.withSchema(ERROR_SCHEMA).addValues(e.toString(), recordString).build());
        }
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  private static final HashMap<String, SpannerSchema> TABLE_SCHEMAS = new HashMap<>();

  private static Schema getTableSchema(SpannerChangestreamsReadConfiguration config) {
    Pipeline miniPipeline = Pipeline.create();
    PCollectionView<Dialect> sqlDialectView =
        miniPipeline
            .apply("Create Dialect", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("Dialect to View", View.asSingleton());
    miniPipeline
        .apply(Create.of((Void) null))
        .apply(
            ParDo.of(
                    new ReadSpannerSchema(
                        SpannerConfig.create()
                            .withDatabaseId(config.getDatabaseId())
                            .withInstanceId(config.getInstanceId())
                            .withProjectId(config.getProjectId()),
                        sqlDialectView,
                        Sets.newHashSet(config.getTable())))
                .withSideInput("dialect", sqlDialectView))
        .apply(
            ParDo.of(
                new DoFn<SpannerSchema, String>() {
                  @ProcessElement
                  public void process(@DoFn.Element SpannerSchema schema) {
                    TABLE_SCHEMAS.put(config.getTable(), schema);
                  }
                }))
        .setCoder(StringUtf8Coder.of());
    miniPipeline.run().waitUntilFinish();
    // Clean up the static map from the object.
    SpannerSchema finalSchemaObj = TABLE_SCHEMAS.remove(config.getTable());
    if (finalSchemaObj == null) {
      throw new RuntimeException(
          String.format("Could not get schema for configuration %s", config));
    }
    return spannerSchemaToBeamSchema(finalSchemaObj, config.getTable());
  }

  private static Schema spannerSchemaToBeamSchema(
      SpannerSchema spannerSchema, final String tableName) {
    OptionalInt optionalIdx =
        IntStream.range(0, spannerSchema.getTables().size())
            .filter(idx -> spannerSchema.getTables().get(idx).equalsIgnoreCase(tableName))
            .findAny();
    if (!optionalIdx.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to retrieve schema for table %s. Found only tables: [%s]",
              tableName, String.join(", ", spannerSchema.getTables())));
    }
    Schema.Builder schemaBuilder = Schema.builder();

    String spannerTableName = spannerSchema.getTables().get(optionalIdx.getAsInt());

    for (SpannerSchema.Column col : spannerSchema.getColumns(spannerTableName)) {
      schemaBuilder =
          schemaBuilder.addNullableField(col.getName(), spannerTypeToBeamType(col.getType()));
    }

    schemaBuilder =
        schemaBuilder.setOptions(
            Schema.Options.builder()
                .setOption(
                    "primaryKeyColumns",
                    Schema.FieldType.array(Schema.FieldType.STRING),
                    spannerSchema.getKeyParts(spannerTableName).stream()
                        .map(SpannerSchema.KeyPart::getField)
                        .collect(Collectors.toList())));

    return Schema.builder()
        .addStringField("operation")
        .addStringField("commitTimestamp")
        .addInt64Field("recordSequence")
        .addRowField("rowValues", schemaBuilder.build())
        .build();
  }

  private static Object stringToParsedValue(Schema.FieldType fieldType, String fieldValue) {
    switch (fieldType.getTypeName()) {
      case STRING:
        return fieldValue;
      case INT64:
        return Long.valueOf(fieldValue);
      case INT16:
      case INT32:
        return Integer.valueOf(fieldValue);
      case FLOAT:
        return Float.parseFloat(fieldValue);
      case DOUBLE:
        return Double.parseDouble(fieldValue);
      case BOOLEAN:
        return Boolean.parseBoolean(fieldValue);
      case BYTES:
        return fieldValue.getBytes(StandardCharsets.UTF_8);
      case DATETIME:
        return new DateTime(fieldValue);
      case DECIMAL:
        return new BigDecimal(fieldValue);
      default:
        throw new IllegalArgumentException(
            String.format("Unable to parse field with type %s", fieldType));
    }
  }

  private static Schema.FieldType spannerTypeToBeamType(Type spannerType) {
    switch (spannerType.getCode()) {
      case BOOL:
        return Schema.FieldType.BOOLEAN;
      case BYTES:
        return Schema.FieldType.BYTES;
      case STRING:
        return Schema.FieldType.STRING;
      case INT64:
        return Schema.FieldType.INT64;
      case NUMERIC:
        return Schema.FieldType.DECIMAL;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case TIMESTAMP:
      case DATE:
        return Schema.FieldType.DATETIME;
      case ARRAY:
        return Schema.FieldType.array(spannerTypeToBeamType(spannerType.getArrayElementType()));
      case JSON:
      case STRUCT:
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported spanner type: %s", spannerType));
    }
  }
}
