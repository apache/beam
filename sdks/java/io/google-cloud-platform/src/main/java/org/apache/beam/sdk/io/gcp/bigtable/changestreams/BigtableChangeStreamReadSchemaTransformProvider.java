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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.data.v2.models.AddToCell;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.MergeToCell;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.bigtable.data.v2.models.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link TypedSchemaTransformProvider} for reading Cloud Bigtable change streams.
 *
 * <p>Internal only.
 */
@AutoService(SchemaTransformProvider.class)
public class BigtableChangeStreamReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        BigtableChangeStreamReadSchemaTransformProvider.BigtableChangeStreamReadConfiguration> {

  private static final String OUTPUT_TAG = "output";

  static final Schema VALUE_SCHEMA =
      Schema.builder()
          .addStringField("type")
          .addNullableInt64Field("int_value")
          .addNullableInt64Field("raw_timestamp_micros")
          .addNullableByteArrayField("raw_value")
          .build();

  static final Schema TIMESTAMP_RANGE_SCHEMA =
      Schema.builder()
          .addStringField("start_bound")
          .addNullableInt64Field("start_timestamp_micros")
          .addStringField("end_bound")
          .addNullableInt64Field("end_timestamp_micros")
          .build();

  static final Schema ENTRY_SCHEMA =
      Schema.builder()
          .addStringField("type")
          .addStringField("family_name")
          .addNullableByteArrayField("qualifier")
          .addNullableInt64Field("timestamp_micros")
          .addNullableByteArrayField("value")
          .addNullableRowField("timestamp_range", TIMESTAMP_RANGE_SCHEMA)
          .addNullableRowField("value_qualifier", VALUE_SCHEMA)
          .addNullableRowField("value_timestamp", VALUE_SCHEMA)
          .addNullableRowField("value_input", VALUE_SCHEMA)
          .build();

  static final Schema CHANGE_STREAM_MUTATION_SCHEMA =
      Schema.builder()
          .addByteArrayField("row_key")
          .addStringField("mutation_type")
          .addStringField("source_cluster_id")
          .addDateTimeField("commit_timestamp")
          .addInt32Field("tie_breaker")
          .addStringField("token")
          .addDateTimeField("estimated_low_watermark")
          .addArrayField("entries", Schema.FieldType.row(ENTRY_SCHEMA))
          .build();

  @Override
  protected Class<BigtableChangeStreamReadConfiguration> configurationClass() {
    return BigtableChangeStreamReadConfiguration.class;
  }

  @Override
  protected SchemaTransform from(BigtableChangeStreamReadConfiguration configuration) {
    return new BigtableChangeStreamReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigtable_cdc_read:v1";
  }

  @Override
  public String description() {
    return "Reads change stream records from a Google Cloud Bigtable table.";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /** Configuration for reading a Bigtable change stream. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigtableChangeStreamReadConfiguration implements Serializable {

    public void validate() {
      checkArgument(!getTableId().isEmpty(), "Bigtable table ID must not be empty.");
      checkArgument(!getInstanceId().isEmpty(), "Bigtable instance ID must not be empty.");
      checkArgument(!getProjectId().isEmpty(), "Bigtable project ID must not be empty.");
    }

    @SchemaFieldDescription("Google Cloud project ID containing the Bigtable instance.")
    public abstract String getProjectId();

    @SchemaFieldDescription("Bigtable instance ID to connect to.")
    public abstract String getInstanceId();

    @SchemaFieldDescription("Bigtable table ID whose change stream should be read.")
    public abstract String getTableId();

    @SchemaFieldDescription("Bigtable app profile used to read the change stream.")
    public abstract @Nullable String getAppProfileId();

    @SchemaFieldDescription("Timestamp from which to start reading the change stream.")
    public abstract @Nullable String getStartAtTimestamp();

    @SchemaFieldDescription("Name used to identify the Bigtable change stream pipeline.")
    public abstract @Nullable String getChangeStreamName();

    public static Builder builder() {
      return new AutoValue_BigtableChangeStreamReadSchemaTransformProvider_BigtableChangeStreamReadConfiguration
          .Builder();
    }

    /** Builder for {@link BigtableChangeStreamReadConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setProjectId(String projectId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setTableId(String tableId);

      public abstract Builder setAppProfileId(String appProfileId);

      public abstract Builder setStartAtTimestamp(String startAtTimestamp);

      public abstract Builder setChangeStreamName(String changeStreamName);

      public abstract BigtableChangeStreamReadConfiguration build();
    }
  }

  /** SchemaTransform implementation for Bigtable change stream reads. */
  private static class BigtableChangeStreamReadSchemaTransform extends SchemaTransform {

    private final BigtableChangeStreamReadConfiguration configuration;

    BigtableChangeStreamReadSchemaTransform(BigtableChangeStreamReadConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty(),
          String.format(
              "Input to %s is expected to be empty, but is not.", getClass().getSimpleName()));

      BigtableIO.ReadChangeStream readChangeStream =
          BigtableIO.readChangeStream()
              .withProjectId(configuration.getProjectId())
              .withInstanceId(configuration.getInstanceId())
              .withTableId(configuration.getTableId());

      @Nullable String appProfileId = configuration.getAppProfileId();
      if (appProfileId != null) {
        readChangeStream = readChangeStream.withAppProfileId(appProfileId);
      }

      @Nullable String startAtTimestamp = configuration.getStartAtTimestamp();
      if (startAtTimestamp != null) {
        readChangeStream = readChangeStream.withStartTime(Instant.parse(startAtTimestamp));
      }

      @Nullable String changeStreamName = configuration.getChangeStreamName();
      if (changeStreamName != null) {
        readChangeStream = readChangeStream.withChangeStreamName(changeStreamName);
      }

      PCollection<KV<ByteString, ChangeStreamMutation>> mutations =
          input.getPipeline().apply(readChangeStream);

      PCollection<Row> rows =
          mutations
              .apply("ConvertToBeamRows", ParDo.of(new ChangeStreamMutationToRowDoFn()))
              .setRowSchema(CHANGE_STREAM_MUTATION_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, rows);
    }
  }

  private static class ChangeStreamMutationToRowDoFn
      extends DoFn<KV<ByteString, ChangeStreamMutation>, Row> {

    @ProcessElement
    public void processElement(
        @Element KV<ByteString, ChangeStreamMutation> element, OutputReceiver<Row> out) {
      out.output(mutationToRow(element.getValue()));
    }
  }

  private static Row mutationToRow(ChangeStreamMutation mutation) {
    List<Row> entries = new ArrayList<>();

    for (Entry entry : mutation.getEntries()) {
      entries.add(entryToRow(entry));
    }

    return Row.withSchema(CHANGE_STREAM_MUTATION_SCHEMA)
        .addValue(mutation.getRowKey().toByteArray())
        .addValue(mutation.getType().name())
        .addValue(mutation.getSourceClusterId())
        .addValue(new Instant(mutation.getCommitTime().toEpochMilli()))
        .addValue(mutation.getTieBreaker())
        .addValue(mutation.getToken())
        .addValue(new Instant(mutation.getEstimatedLowWatermarkTime().toEpochMilli()))
        .addValue(entries)
        .build();
  }

  private static Row entryToRow(Entry entry) {
    if (entry instanceof SetCell) {
      SetCell setCell = (SetCell) entry;

      return Row.withSchema(ENTRY_SCHEMA)
          .addValue("SET_CELL")
          .addValue(setCell.getFamilyName())
          .addValue(setCell.getQualifier().toByteArray())
          .addValue(setCell.getTimestamp())
          .addValue(setCell.getValue().toByteArray())
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();
    }

    if (entry instanceof DeleteCells) {
      DeleteCells deleteCells = (DeleteCells) entry;

      return Row.withSchema(ENTRY_SCHEMA)
          .addValue("DELETE_CELLS")
          .addValue(deleteCells.getFamilyName())
          .addValue(deleteCells.getQualifier().toByteArray())
          .addValue(null)
          .addValue(null)
          .addValue(timestampRangeToRow(deleteCells.getTimestampRange()))
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();
    }

    if (entry instanceof DeleteFamily) {
      DeleteFamily deleteFamily = (DeleteFamily) entry;

      return Row.withSchema(ENTRY_SCHEMA)
          .addValue("DELETE_FAMILY")
          .addValue(deleteFamily.getFamilyName())
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();
    }

    if (entry instanceof AddToCell) {
      AddToCell addToCell = (AddToCell) entry;

      return Row.withSchema(ENTRY_SCHEMA)
          .addValue("ADD_TO_CELL")
          .addValue(addToCell.getFamily())
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(valueToRow(addToCell.getQualifier()))
          .addValue(valueToRow(addToCell.getTimestamp()))
          .addValue(valueToRow(addToCell.getInput()))
          .build();
    }

    if (entry instanceof MergeToCell) {
      MergeToCell mergeToCell = (MergeToCell) entry;

      return Row.withSchema(ENTRY_SCHEMA)
          .addValue("MERGE_TO_CELL")
          .addValue(mergeToCell.getFamily())
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(valueToRow(mergeToCell.getQualifier()))
          .addValue(valueToRow(mergeToCell.getTimestamp()))
          .addValue(valueToRow(mergeToCell.getInput()))
          .build();
    }

    throw new IllegalArgumentException(
        "Unsupported Bigtable change stream entry: " + entry.getClass().getName());
  }

  private static Row timestampRangeToRow(Range.TimestampRange range) {
    @Nullable Long startTimestampMicros =
        range.getStartBound() == Range.BoundType.UNBOUNDED ? null : range.getStart();

    @Nullable Long endTimestampMicros =
        range.getEndBound() == Range.BoundType.UNBOUNDED ? null : range.getEnd();

    return Row.withSchema(TIMESTAMP_RANGE_SCHEMA)
        .addValue(range.getStartBound().name())
        .addValue(startTimestampMicros)
        .addValue(range.getEndBound().name())
        .addValue(endTimestampMicros)
        .build();
  }

  private static Row valueToRow(Value value) {
    switch (value.getValueType()) {
      case Int64:
        return Row.withSchema(VALUE_SCHEMA)
            .addValue("INT64")
            .addValue(((Value.IntValue) value).getValue())
            .addValue(null)
            .addValue(null)
            .build();

      case RawTimestamp:
        return Row.withSchema(VALUE_SCHEMA)
            .addValue("RAW_TIMESTAMP")
            .addValue(null)
            .addValue(((Value.RawTimestamp) value).getValue())
            .addValue(null)
            .build();

      case RawValue:
        return Row.withSchema(VALUE_SCHEMA)
            .addValue("RAW_VALUE")
            .addValue(null)
            .addValue(null)
            .addValue(((Value.RawValue) value).getValue().toByteArray())
            .build();

      default:
        throw new IllegalArgumentException(
            "Unsupported Bigtable Value type: " + value.getValueType());
    }
  }
}
