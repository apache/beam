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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class PubsubLiteWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteWriteSchemaTransformProvider.PubsubLiteWriteSchemaTransformConfiguration> {

  public static final String SUPPORTED_FORMATS_STR = "JSON,AVRO";
  public static final Set<String> SUPPORTED_FORMATS =
      Sets.newHashSet(SUPPORTED_FORMATS_STR.split(","));
  public static final TupleTag<PubSubMessage> OUTPUT_TAG = new TupleTag<PubSubMessage>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableByteArrayField("row").build();
  private static final Logger LOG =
      LoggerFactory.getLogger(PubsubLiteWriteSchemaTransformProvider.class);

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<PubsubLiteWriteSchemaTransformConfiguration>
      configurationClass() {
    return PubsubLiteWriteSchemaTransformConfiguration.class;
  }

  public static class ErrorCounterFn extends DoFn<Row, PubSubMessage> {
    private SerializableFunction<Row, byte[]> toBytesFn;
    private Counter errorCounter;
    private long errorsInBundle = 0L;

    public ErrorCounterFn(String name, SerializableFunction<Row, byte[]> toBytesFn) {
      this.toBytesFn = toBytesFn;
      errorCounter = Metrics.counter(PubsubLiteWriteSchemaTransformProvider.class, name);
    }

    @ProcessElement
    public void process(@DoFn.Element Row row, MultiOutputReceiver receiver) {
      try {
        PubSubMessage message =
            PubSubMessage.newBuilder()
                .setData(ByteString.copyFrom(Objects.requireNonNull(toBytesFn.apply(row))))
                .build();

        receiver.get(OUTPUT_TAG).output(message);
      } catch (Exception e) {
        errorsInBundle += 1;
        LOG.warn("Error while parsing the element", e);
        receiver
            .get(ERROR_TAG)
            .output(
                Row.withSchema(ERROR_SCHEMA)
                    .addValues(e.toString(), row.toString().getBytes(StandardCharsets.UTF_8))
                    .build());
      }
    }

    @FinishBundle
    public void finish() {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
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
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Schema inputSchema = input.get("input").getSchema();
        final SerializableFunction<Row, byte[]> toBytesFn =
            configuration.getFormat().equals("JSON")
                ? JsonUtils.getRowToJsonBytesFunction(inputSchema)
                : AvroUtils.getRowToAvroBytesFunction(inputSchema);

        PCollectionTuple outputTuple =
            input
                .get("input")
                .apply(
                    "Map Rows to PubSubMessages",
                    ParDo.of(new ErrorCounterFn("PubSubLite-write-error-counter", toBytesFn))
                        .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

        outputTuple
            .get(OUTPUT_TAG)
            .apply("Add UUIDs", PubsubLiteIO.addUuids())
            .apply(
                "Write to PS Lite",
                PubsubLiteIO.write(
                    PublisherOptions.newBuilder()
                        .setTopicPath(
                            TopicPath.newBuilder()
                                .setProject(ProjectId.of(configuration.getProject()))
                                .setName(TopicName.of(configuration.getTopicName()))
                                .setLocation(CloudRegionOrZone.parse(configuration.getLocation()))
                                .build())
                        .build()));

        return PCollectionRowTuple.of(
            "errors", outputTuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
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
    return Collections.singletonList("errors");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteWriteSchemaTransformConfiguration {
    @SchemaFieldDescription(
        "The GCP project where the Pubsub Lite reservation resides. This can be a "
            + "project number of a project ID.")
    public abstract String getProject();

    @SchemaFieldDescription("The region or zone where the Pubsub Lite reservation resides.")
    public abstract String getLocation();

    @SchemaFieldDescription(
        "The name of the topic to publish data into. This will be concatenated with "
            + "the project and location parameters to build a full topic path.")
    public abstract String getTopicName();

    @SchemaFieldDescription(
        "The encoding format for the data stored in Pubsub Lite. Valid options are: "
            + SUPPORTED_FORMATS_STR)
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
