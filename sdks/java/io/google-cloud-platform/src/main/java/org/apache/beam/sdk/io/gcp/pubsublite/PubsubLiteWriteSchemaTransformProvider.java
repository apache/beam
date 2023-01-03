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
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class PubsubLiteWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteWriteSchemaTransformProvider.PubsubLiteWriteSchemaTransformConfiguration> {

  public static final Set<String> SUPPORTED_FORMATS = Sets.newHashSet("JSON", "AVRO");

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<PubsubLiteWriteSchemaTransformConfiguration>
      configurationClass() {
    return PubsubLiteWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      PubsubLiteWriteSchemaTransformConfiguration configuration) {

    if (!SUPPORTED_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          "Format "
              + configuration.getFormat()
              + " is not supported. "
              + "Supported formats are: "
              + String.join(", ", SUPPORTED_FORMATS));
    }

    return new SchemaTransform() {

      @Override
      public @UnknownKeyFor @NonNull @Initialized PTransform<
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
              @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
          buildTransform() {
        return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
          @Override
          public PCollectionRowTuple expand(PCollectionRowTuple input) {
            Schema inputSchema = input.get("input").getSchema();
            final SerializableFunction<Row, byte[]> toBytesFn =
                configuration.getFormat().equals("JSON")
                    ? JsonUtils.getRowToJsonBytesFunction(inputSchema)
                    : AvroUtils.getRowToAvroBytesFunction(inputSchema);
            input
                .get("input")
                .apply(
                    "Map Rows to PubSubMessages",
                    MapElements.into(TypeDescriptor.of(PubSubMessage.class))
                        .via(
                            row ->
                                PubSubMessage.newBuilder()
                                    .setData(
                                        ByteString.copyFrom(
                                            Objects.requireNonNull(toBytesFn.apply(row))))
                                    .build()))
                .apply("Add UUIDs", PubsubLiteIO.addUuids())
                .apply(
                    "Write to PS Lite",
                    PubsubLiteIO.write(
                        PublisherOptions.newBuilder()
                            .setTopicPath(
                                TopicPath.newBuilder()
                                    .setProject(ProjectId.of(configuration.getProject()))
                                    .setName(TopicName.of(configuration.getTopicName()))
                                    .setLocation(
                                        CloudRegionOrZone.parse(configuration.getLocation()))
                                    .build())
                            .build()));
            return PCollectionRowTuple.empty(input.getPipeline());
          }
        };
      }
    };
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:pubsublite_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.emptyList();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteWriteSchemaTransformConfiguration {
    public abstract String getProject();

    public abstract String getLocation();

    public abstract String getTopicName();

    public abstract String getFormat();

    public static Builder builder() {
      return new AutoValue_PubsubLiteWriteSchemaTransformProvider_PubsubLiteWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProject(String project);

      public abstract Builder setLocation(String location);

      public abstract Builder setTopicName(String topicName);

      public abstract Builder setFormat(String format);

      public abstract PubsubLiteWriteSchemaTransformConfiguration build();
    }
  }
}
