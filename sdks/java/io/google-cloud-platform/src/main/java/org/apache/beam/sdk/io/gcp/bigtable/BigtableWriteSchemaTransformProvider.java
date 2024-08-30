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
package org.apache.beam.sdk.io.gcp.bigtable;

import static java.util.Optional.ofNullable;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Bigtable Write jobs configured via
 * {@link BigtableWriteSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class BigtableWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigtableWriteSchemaTransformConfiguration> {

  private static final String INPUT_TAG = "input";

  @Override
  protected SchemaTransform from(BigtableWriteSchemaTransformConfiguration configuration) {
    return new BigtableWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigtable_write:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /** Configuration for writing to Bigtable. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class BigtableWriteSchemaTransformConfiguration {
    /** Instantiates a {@link BigtableWriteSchemaTransformConfiguration.Builder} instance. */
    public static Builder builder() {
      return new AutoValue_BigtableWriteSchemaTransformProvider_BigtableWriteSchemaTransformConfiguration
          .Builder();
    }

    /** Validates the configuration object. */
    public void validate() {
      String invalidConfigMessage =
          "Invalid Bigtable Write configuration: %s should be a non-empty String";
      checkArgument(!this.getTableId().isEmpty(), String.format(invalidConfigMessage, "table"));
      checkArgument(
          !this.getInstanceId().isEmpty(), String.format(invalidConfigMessage, "instance"));
      checkArgument(!this.getProjectId().isEmpty(), String.format(invalidConfigMessage, "project"));
    }

    public abstract String getTableId();

    public abstract String getInstanceId();

    public abstract String getProjectId();

    /** Builder for the {@link BigtableWriteSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTableId(String tableId);

      public abstract Builder setInstanceId(String instanceId);

      public abstract Builder setProjectId(String projectId);

      /** Builds a {@link BigtableWriteSchemaTransformConfiguration} instance. */
      public abstract BigtableWriteSchemaTransformConfiguration build();
    }
  }

  /**
   * A {@link SchemaTransform} for Bigtable writes, configured with {@link
   * BigtableWriteSchemaTransformConfiguration} and instantiated by {@link
   * BigtableWriteSchemaTransformProvider}.
   */
  private static class BigtableWriteSchemaTransform extends SchemaTransform {
    private final BigtableWriteSchemaTransformConfiguration configuration;

    BigtableWriteSchemaTransform(BigtableWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.has(INPUT_TAG),
          String.format(
              "Could not find expected input [%s] to %s.", INPUT_TAG, getClass().getSimpleName()));

      PCollection<Row> beamRowMutations = input.get(INPUT_TAG);
      PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations =
          beamRowMutations.apply(MapElements.via(new GetMutationsFromBeamRow()));

      bigtableMutations.apply(
          BigtableIO.write()
              .withTableId(configuration.getTableId())
              .withInstanceId(configuration.getInstanceId())
              .withProjectId(configuration.getProjectId()));

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  public static class GetMutationsFromBeamRow
      extends SimpleFunction<Row, KV<ByteString, Iterable<Mutation>>> {
    @Override
    public KV<ByteString, Iterable<Mutation>> apply(Row row) {
      ByteString key = ByteString.copyFrom(ofNullable(row.getBytes("key")).get());
      List<Map<String, byte[]>> beamRowMutations =
          (List) ofNullable(row.getArray("mutations")).get();

      List<Mutation> mutations = new ArrayList<>(beamRowMutations.size());

      for (Map<String, byte[]> mutation : beamRowMutations) {
        Mutation bigtableMutation;
        switch (new String(ofNullable(mutation.get("type")).get(), StandardCharsets.UTF_8)) {
          case "SetCell":
            Mutation.SetCell.Builder setMutation =
                Mutation.SetCell.newBuilder()
                    .setValue(ByteString.copyFrom(ofNullable(mutation.get("value")).get()))
                    .setColumnQualifier(
                        ByteString.copyFrom(ofNullable(mutation.get("column_qualifier")).get()))
                    .setFamilyNameBytes(
                        ByteString.copyFrom(ofNullable(mutation.get("family_name")).get()))
                    // Use timestamp if provided, else default to -1 (current Bigtable server time)
                    .setTimestampMicros(
                        mutation.containsKey("timestamp_micros")
                            ? Longs.fromByteArray(
                                ofNullable(mutation.get("timestamp_micros")).get())
                            : -1);
            bigtableMutation = Mutation.newBuilder().setSetCell(setMutation.build()).build();
            break;
          case "DeleteFromColumn":
            Mutation.DeleteFromColumn.Builder deleteMutation =
                Mutation.DeleteFromColumn.newBuilder()
                    .setColumnQualifier(
                        ByteString.copyFrom(ofNullable(mutation.get("column_qualifier")).get()))
                    .setFamilyNameBytes(
                        ByteString.copyFrom(ofNullable(mutation.get("family_name")).get()));

            // set timestamp range if applicable
            if (mutation.containsKey("start_timestamp_micros")
                || mutation.containsKey("end_timestamp_micros")) {
              TimestampRange.Builder timeRange = TimestampRange.newBuilder();
              if (mutation.containsKey("start_timestamp_micros")) {
                Long startMicros =
                    ByteBuffer.wrap(ofNullable(mutation.get("start_timestamp_micros")).get())
                        .getLong();
                timeRange.setStartTimestampMicros(startMicros);
              }
              if (mutation.containsKey("end_timestamp_micros")) {
                Long endMicros =
                    ByteBuffer.wrap(ofNullable(mutation.get("end_timestamp_micros")).get())
                        .getLong();
                timeRange.setEndTimestampMicros(endMicros);
              }
              deleteMutation.setTimeRange(timeRange.build());
            }
            bigtableMutation =
                Mutation.newBuilder().setDeleteFromColumn(deleteMutation.build()).build();
            break;
          case "DeleteFromFamily":
            bigtableMutation =
                Mutation.newBuilder()
                    .setDeleteFromFamily(
                        Mutation.DeleteFromFamily.newBuilder()
                            .setFamilyNameBytes(
                                ByteString.copyFrom(ofNullable(mutation.get("family_name")).get()))
                            .build())
                    .build();
            break;
          case "DeleteFromRow":
            bigtableMutation =
                Mutation.newBuilder()
                    .setDeleteFromRow(Mutation.DeleteFromRow.newBuilder().build())
                    .build();
            break;
          default:
            throw new RuntimeException(
                String.format(
                    "Unexpected mutation type [%s]: %s",
                    Arrays.toString(ofNullable(mutation.get("type")).get()), mutation));
        }
        mutations.add(bigtableMutation);
      }
      return KV.of(key, mutations);
    }
  }
}
