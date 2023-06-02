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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;

import java.util.*;

import com.google.bigtable.v2.Mutation;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteSchemaTransformProvider.BigtableWriteSchemaTransformConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

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
    protected Class<BigtableWriteSchemaTransformConfiguration> configurationClass() {
        return BigtableWriteSchemaTransformConfiguration.class;
    }

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

    @Override
    public List<String> outputCollectionNames() {
        return Collections.emptyList();
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

        public abstract String getTable();

        public abstract String getInstance();

        public abstract String getProject();

        /** Builder for the {@link BigtableWriteSchemaTransformConfiguration}. */
        @AutoValue.Builder
        public abstract static class Builder {
            public abstract Builder setTable(String table);

            public abstract Builder setInstance(String instance);

            public abstract Builder setProject(String project);

            abstract BigtableWriteSchemaTransformConfiguration autoBuild();

            /** Builds a {@link BigtableWriteSchemaTransformConfiguration} instance. */
            public BigtableWriteSchemaTransformConfiguration build() {
                BigtableWriteSchemaTransformConfiguration config = autoBuild();

                String invalidConfigMessage =
                        "Invalid Bigtable Write configuration: %s should be a non-empty String";
                checkArgument(!config.getTable().isEmpty(), String.format(invalidConfigMessage, "table"));
                checkArgument(
                        !config.getInstance().isEmpty(), String.format(invalidConfigMessage, "instance"));
                checkArgument(
                        !config.getProject().isEmpty(), String.format(invalidConfigMessage, "project"));

                return config;
            };
        }
    }

    /**
     * A {@link SchemaTransform} for Bigtable writes, configured with {@link
     * BigtableWriteSchemaTransformConfiguration} and instantiated by {@link
     * BigtableWriteSchemaTransformProvider}.
     */
    private static class BigtableWriteSchemaTransform
            extends PTransform<PCollectionRowTuple, PCollectionRowTuple> implements SchemaTransform {
        private final BigtableWriteSchemaTransformConfiguration configuration;

        BigtableWriteSchemaTransform(BigtableWriteSchemaTransformConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
            checkArgument(
                    input.has(INPUT_TAG),
                    String.format(
                            "Could not find expected input [%s] to %s.", INPUT_TAG, getClass().getSimpleName()));

            PCollection<Row> beamRowMutations = input.get(INPUT_TAG);
            PCollection<KV<ByteString, Iterable<Mutation>>> bigtableMutations = beamRowMutations
                    .apply(MapElements.via(new GetMutationsFromBeamRow()));

            bigtableMutations.apply(BigtableIO.write()
                    .withTableId(configuration.getTable())
                    .withInstanceId(configuration.getInstance())
                    .withProjectId(configuration.getProject()));

            return PCollectionRowTuple.empty(input.getPipeline());
        }

        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
            return this;
        }
    }

    public static class GetMutationsFromBeamRow extends SimpleFunction<Row, KV<ByteString, Iterable<Mutation>>> {
        @Override
        public KV<ByteString, Iterable<Mutation>> apply(Row row) {
            ByteString key = ByteString.copyFrom(row.getBytes("key"));
            List<Map<String, byte[]>> beamRowMutations = (List) row.getArray("mutations");

            List<Mutation> mutations = new ArrayList<>(beamRowMutations.size());

            for (Map<String, byte[]> mutation: beamRowMutations) {
                Mutation bigtableMutation;
                switch (Arrays.toString(mutation.get("type"))){
                    case "SetCell":
                        bigtableMutation = Mutation.newBuilder()
                            .setSetCell(
                                Mutation.SetCell.newBuilder()
                                    .setValue(ByteString.copyFrom(mutation.get("value")))
                                    .setColumnQualifier(ByteString.copyFrom(mutation.get("column_qualifier")))
                                    .setFamilyNameBytes(ByteString.copyFrom(mutation.get("family_name")))
                                    .setTimestampMicros(Longs.fromByteArray(mutation.get("timestamp_micros")))
                                    .build()
                            ).build();
                        break;
                    case "DeleteFromColumn":
                        bigtableMutation = Mutation.newBuilder()
                            .setDeleteFromColumn(
                                Mutation.DeleteFromColumn.newBuilder()
                                    .setColumnQualifier(ByteString.copyFrom(mutation.get("column_qualifier")))
                                    .setFamilyNameBytes(ByteString.copyFrom(mutation.get("family_name")))
                                    .build()
                            ).build();
                        break;
                    case "DeleteFromFamily":
                        bigtableMutation = Mutation.newBuilder()
                            .setDeleteFromFamily(
                                Mutation.DeleteFromFamily.newBuilder()
                                    .setFamilyNameBytes(ByteString.copyFrom(mutation.get("family_name")))
                                    .build()
                            ).build();
                        break;
                    case "DeleteFromRow":
                        bigtableMutation = Mutation.newBuilder()
                            .setDeleteFromRow(Mutation.DeleteFromRow.newBuilder().build()).build();
                        break;
                    default:
                        throw new RuntimeException(String.format("Unexpected mutation type [%s]: %s", Arrays.toString(mutation.get("type")), mutation));
                }
                mutations.add(bigtableMutation);
            }
            return KV.of(key, mutations);
        }
    }
}
