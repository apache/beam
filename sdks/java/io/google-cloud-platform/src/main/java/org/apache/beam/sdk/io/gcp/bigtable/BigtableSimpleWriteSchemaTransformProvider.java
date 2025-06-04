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
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.GroupByKey;
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
public class BigtableSimpleWriteSchemaTransformProvider
    extends BigtableWriteSchemaTransformProvider {

  private static final String INPUT_TAG = "input";

  @Override
  protected SchemaTransform from(BigtableWriteSchemaTransformConfiguration configuration) {
    return new BigtableSimpleWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigtable_simple_write:v1";
  }

  /**
   * A {@link SchemaTransform} for Bigtable writes, configured with {@link
   * BigtableWriteSchemaTransformConfiguration} and instantiated by {@link
   * BigtableWriteSchemaTransformProvider}.
   */
  private static class BigtableSimpleWriteSchemaTransform extends SchemaTransform {
    private final BigtableWriteSchemaTransformConfiguration configuration;

    BigtableSimpleWriteSchemaTransform(BigtableWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.has(INPUT_TAG),
          String.format(
              "Could not find expected input [%s] to %s.", INPUT_TAG, getClass().getSimpleName()));

      PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations =
          changeMutationInput(input);

      bigtableMutations.apply(
          BigtableIO.write()
              .withTableId(configuration.getTableId())
              .withInstanceId(configuration.getInstanceId())
              .withProjectId(configuration.getProjectId()));

      return PCollectionRowTuple.empty(input.getPipeline());
    }

    public PCollection<KV<ByteString, Iterable<Mutation>>> changeMutationInput(
        PCollectionRowTuple input) {
      PCollection<Row> beamRowMutationsList = input.getSinglePCollection();

      // convert all row inputs into KV<ByteString, Mutation>
      PCollection<KV<ByteString, Mutation>> changedBeamRowMutationsList =
          beamRowMutationsList.apply(
              MapElements.via(
                  new SimpleFunction<Row, KV<ByteString, Mutation>>() {
                    @Override
                    public KV<ByteString, Mutation> apply(Row input) {
                      ByteString key = ByteString.copyFrom(ofNullable(input.getBytes("key")).get());
                      Mutation bigtableMutation = simpleInputChange(input);
                      return KV.of(key, bigtableMutation);
                    }
                  }));
      // now we need to make the KV into a PCollection of KV<ByteString, Iterable<Mutation>>
      return changedBeamRowMutationsList.apply(GroupByKey.create());
    }
  }

  // converts a row input into Mutation
  public static Mutation simpleInputChange(Row input) {
    Mutation bigtableMutation;
    switch (new String(ofNullable(input.getBytes("type")).get(), StandardCharsets.UTF_8)) {
      case "SetCell":
        Mutation.SetCell.Builder setMutation =
            Mutation.SetCell.newBuilder()
                .setValue(ByteString.copyFrom(ofNullable(input.getBytes("value")).get()))
                .setColumnQualifier(
                    ByteString.copyFrom(ofNullable(input.getBytes("column_qualifier")).get()))
                .setFamilyNameBytes(
                    ByteString.copyFrom(ofNullable(input.getBytes("family_name")).get()))
                // Use timestamp if provided, else default to -1 (current Bigtable
                // server time)
                .setTimestampMicros(
                    ByteString.copyFrom(ofNullable(input.getBytes("timestamp_micros")).get())
                            .isEmpty()
                        ? Longs.fromByteArray(ofNullable(input.getBytes("timestamp_micros")).get())
                        : -1);
        bigtableMutation = Mutation.newBuilder().setSetCell(setMutation.build()).build();
        break;
      case "DeleteFromColumn":
        // set timestamp range if applicable

        Mutation.DeleteFromColumn.Builder deleteMutation =
            Mutation.DeleteFromColumn.newBuilder()
                .setColumnQualifier(
                    ByteString.copyFrom(ofNullable(input.getBytes("column_qualifier")).get()))
                .setFamilyNameBytes(
                    ByteString.copyFrom(ofNullable(input.getBytes("family_name")).get()));

        // if start or end timestop provided
        if (ByteString.copyFrom(ofNullable(input.getBytes("start_timestamp_micros")).get())
                .isEmpty()
            || ByteString.copyFrom(ofNullable(input.getBytes("end_timestamp_micros")).get())
                .isEmpty()) {
          TimestampRange.Builder timeRange = TimestampRange.newBuilder();
          if (ByteString.copyFrom(ofNullable(input.getBytes("start_timestamp_micros")).get())
              .isEmpty()) {
            Long startMicros =
                ByteBuffer.wrap(ofNullable(input.getBytes("start_timestamp_micros")).get())
                    .getLong();
            timeRange.setStartTimestampMicros(startMicros);
          }
          if (ByteString.copyFrom(ofNullable(input.getBytes("end_timestamp_micros")).get())
              .isEmpty()) {
            Long endMicros =
                ByteBuffer.wrap(ofNullable(input.getBytes("end_timestamp_micros")).get()).getLong();
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
                            ByteString.copyFrom(ofNullable(input.getBytes("family_name")).get()))
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
                Arrays.toString(ofNullable(input.getBytes("type")).get()), input));
    }
    return bigtableMutation;
  }
}
