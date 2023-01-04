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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class PubsubLiteReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteReadSchemaTransformProvider.PubsubLiteReadSchemaTransformConfiguration> {

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<PubsubLiteReadSchemaTransformConfiguration>
      configurationClass() {
    return PubsubLiteReadSchemaTransformConfiguration.class;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      PubsubLiteReadSchemaTransformConfiguration configuration) {
    final Schema beamSchema =
        Objects.equals(configuration.getDataFormat(), "JSON")
            ? JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema())
            : AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(configuration.getSchema()));
    final SerializableFunction<byte[], Row> valueMapper =
        Objects.equals(configuration.getDataFormat(), "JSON")
            ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
            : AvroUtils.getAvroBytesToRowFunction(beamSchema);
    return new SchemaTransform() {
      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            String project = configuration.getProject();
            if (Strings.isNullOrEmpty(project)) {
              project = input.getPipeline().getOptions().as(GcpOptions.class).getProject();
            }
            if (project == null) {
              throw new IllegalArgumentException(
                  "Unable to infer the project to read from Pubsub Lite. Please provide a project.");
            }
            return PCollectionRowTuple.of(
                "output",
                input
                    .getPipeline()
                    .apply(
                        PubsubLiteIO.read(
                            SubscriberOptions.newBuilder()
                                .setSubscriptionPath(
                                    SubscriptionPath.newBuilder()
                                        .setLocation(
                                            CloudRegionOrZone.parse(configuration.getLocation()))
                                        .setProject(ProjectId.of(project))
                                        .setName(
                                            SubscriptionName.of(
                                                configuration.getSubscriptionName()))
                                        .build())
                                .build()))
                    .apply(
                        MapElements.into(TypeDescriptors.rows())
                            .via(
                                seqMess ->
                                    valueMapper.apply(
                                        seqMess.getMessage().getData().toByteArray())))
                    .setRowSchema(beamSchema));
          }
        };
      }
    };
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:pubsublite_read:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteReadSchemaTransformConfiguration {
    public abstract String getDataFormat();

    public abstract String getSchema();

    public abstract @Nullable String getProject();

    public abstract String getSubscriptionName();

    public abstract String getLocation();

    public static Builder builder() {
      return new AutoValue_PubsubLiteReadSchemaTransformProvider_PubsubLiteReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDataFormat(String dataFormat);

      public abstract Builder setSchema(String schema);

      public abstract Builder setProject(String project);

      public abstract Builder setSubscriptionName(String subscriptionName);

      public abstract Builder setLocation(String location);

      public abstract PubsubLiteReadSchemaTransformConfiguration build();
    }
  }
}
