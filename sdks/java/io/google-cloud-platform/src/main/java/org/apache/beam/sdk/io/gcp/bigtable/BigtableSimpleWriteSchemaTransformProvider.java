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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.TimestampRange;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Bigtable Write jobs configured via
 * {@link BigtableWriteSchemaTransformConfiguration}.
 */
@AutoService(SchemaTransformProvider.class)
public class BigtableSimpleWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigtableWriteSchemaTransformConfiguration> {

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

      Schema testOriginialSchema =
          Schema.builder()
              .addByteArrayField("key")
              .addArrayField(
                  "mutations",
                  Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES))
              .build();

      Schema inputSchema = input.getSinglePCollection().getSchema();

      System.out.println("Input Schema for BigTableMutations: " + inputSchema);

      PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations = null;
      if (inputSchema.equals(testOriginialSchema)) {
        PCollection<Row> beamRowMutations = input.get(INPUT_TAG);
        bigtableMutations =
            beamRowMutations.apply(
                // Original schema inputs gets sent out to the original transform provider mutations
                // function
                MapElements.via(
                    new BigtableWriteSchemaTransformProvider.GetMutationsFromBeamRow()));
      } else if (inputSchema.hasField("type")) {
        // new schema inputs get sent to the new transform provider mutation function
        bigtableMutations = changeMutationInput(input);
      } else {
        System.out.println(
            "Inputted Schema is Invalid; the schema should be formatted in one of two ways:\n "
                + "key\": ByteString\n"
                + "\"type\": String\n"
                + "\"column_qualifier\": ByteString\n"
                + "\"family_name\": ByteString\n"
                + "\"timestamp_micros\": Long\n"
                + "\"start_timestamp_micros\": Long\n"
                + "\"end_timestamp_micros\": Long"
                + "OR\n"
                + "\n"
                + "\"key\": ByteString\n"
                + "(\"mutations\", contains map(String, ByteString) of mutations in the mutation schema format");
      }

      if (bigtableMutations != null) {
        bigtableMutations.apply(
            BigtableIO.write()
                .withTableId(configuration.getTableId())
                .withInstanceId(configuration.getInstanceId())
                .withProjectId(configuration.getProjectId()));
      } else {
        checkArgument(
            true,
            "Inputted Schema caused mutation error, check error logs and input schema format");
      }
      return PCollectionRowTuple.empty(input.getPipeline());
    }

    public PCollection<KV<ByteString, Iterable<Mutation>>> changeMutationInput(
        PCollectionRowTuple inputR) {
      PCollection<Row> beamRowMutationsList = inputR.getSinglePCollection();
      // convert all row inputs into KV<ByteString, Mutation>
      PCollection<KV<ByteString, Mutation>> changedBeamRowMutationsList =
          beamRowMutationsList.apply(
              MapElements.into(
                      TypeDescriptors.kvs(
                          TypeDescriptor.of(ByteString.class), TypeDescriptor.of(Mutation.class)))
                  .via(
                      (Row input) -> {
                        @SuppressWarnings("nullness")
                        ByteString key =
                            ByteString.copyFrom(
                                Preconditions.checkStateNotNull(
                                    input.getBytes("key"),
                                    "Encountered row with incorrect 'key' property."));

                        Mutation bigtableMutation;
                        String mutationType =
                            input.getString("type"); // Direct call, can return null
                        if (mutationType == null) {
                          throw new IllegalArgumentException("Mutation type cannot be null.");
                        }
                        switch (mutationType) {
                          case "SetCell":
                            @SuppressWarnings("nullness")
                            Mutation.SetCell.Builder setMutation =
                                Mutation.SetCell.newBuilder()
                                    .setValue(
                                        ByteString.copyFrom(
                                            Preconditions.checkStateNotNull(
                                                input.getBytes("value"),
                                                "Encountered SetCell mutation with incorrect 'value' property.")))
                                    .setColumnQualifier(
                                        ByteString.copyFrom(
                                            Preconditions.checkStateNotNull(
                                                input.getBytes("column_qualifier"),
                                                "Encountered SetCell mutation with incorrect 'column_qualifier' property.")))
                                    .setFamilyNameBytes(
                                        ByteString.copyFrom(
                                            Preconditions.checkStateNotNull(
                                                input.getBytes("family_name"),
                                                "Encountered SetCell mutation with incorrect 'family_name' property.")));
                            // Use timestamp if provided, else default to -1 (current
                            // Bigtable
                            // server time)
                            // Timestamp (optional, assuming Long type in Row schema)
                            Long timestampMicros = input.getInt64("timestamp_micros");
                            setMutation.setTimestampMicros(
                                timestampMicros != null ? timestampMicros : -1);

                            bigtableMutation =
                                Mutation.newBuilder().setSetCell(setMutation.build()).build();
                            break;
                          case "DeleteFromColumn":
                            // set timestamp range if applicable
                            @SuppressWarnings("nullness")
                            Mutation.DeleteFromColumn.Builder deleteMutation =
                                Mutation.DeleteFromColumn.newBuilder()
                                    .setColumnQualifier(
                                        ByteString.copyFrom(
                                            Preconditions.checkStateNotNull(
                                                input.getBytes("column_qualifier"),
                                                "Encountered DeleteFromColumn mutation with incorrect 'column_qualifier' property.")))
                                    .setFamilyNameBytes(
                                        ByteString.copyFrom(
                                            Preconditions.checkStateNotNull(
                                                input.getBytes("family_name"),
                                                "Encountered DeleteFromColumn mutation with incorrect 'family_name' property.")));

                            // if start or end timestamp provided
                            // Timestamp Range (optional, assuming Long type in Row schema)
                            Long startTimestampMicros = null;
                            Long endTimestampMicros = null;

                            if (input.getSchema().hasField("start_timestamp_micros")) {
                              startTimestampMicros = input.getInt64("start_timestamp_micros");
                            }
                            if (input.getSchema().hasField("end_timestamp_micros")) {
                              endTimestampMicros = input.getInt64("end_timestamp_micros");
                            }

                            if (startTimestampMicros != null || endTimestampMicros != null) {
                              TimestampRange.Builder timeRange = TimestampRange.newBuilder();
                              if (startTimestampMicros != null) {
                                timeRange.setStartTimestampMicros(startTimestampMicros);
                              }
                              if (endTimestampMicros != null) {
                                timeRange.setEndTimestampMicros(endTimestampMicros);
                              }
                              deleteMutation.setTimeRange(timeRange.build());
                            }
                            bigtableMutation =
                                Mutation.newBuilder()
                                    .setDeleteFromColumn(deleteMutation.build())
                                    .build();
                            break;
                          case "DeleteFromFamily":
                            bigtableMutation =
                                Mutation.newBuilder()
                                    .setDeleteFromFamily(
                                        Mutation.DeleteFromFamily.newBuilder()
                                            .setFamilyNameBytes(
                                                ByteString.copyFrom(
                                                    Preconditions.checkStateNotNull(
                                                        input.getBytes("family_name"),
                                                        "Encountered DeleteFromFamily mutation with incorrect 'family_name' property.")))
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
                                    ((input.getString("type"))), input));
                        }
                        return KV.of(key, bigtableMutation);
                      }));
      // now we need to make the KV into a PCollection of KV<ByteString, Iterable<Mutation>>
      return changedBeamRowMutationsList.apply(GroupByKey.create());
    }
  }
}
