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
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
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
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class PubsubLiteReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteReadSchemaTransformProvider.PubsubLiteReadSchemaTransformConfiguration> {

  private static final Logger LOG =
      LoggerFactory.getLogger(PubsubLiteReadSchemaTransformProvider.class);

  public static final String VALID_FORMATS_STR = "AVRO,JSON";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableByteArrayField("row").build();

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<PubsubLiteReadSchemaTransformConfiguration>
      configurationClass() {
    return PubsubLiteReadSchemaTransformConfiguration.class;
  }

  public static class ErrorFn extends DoFn<SequencedMessage, Row> {
    private SerializableFunction<byte[], Row> valueMapper;
    private Counter errorCounter;
    private Long errorsInBundle = 0L;

    public ErrorFn(String name, SerializableFunction<byte[], Row> valueMapper) {
      this.errorCounter = Metrics.counter(PubsubLiteReadSchemaTransformProvider.class, name);
      this.valueMapper = valueMapper;
    }

    @ProcessElement
    public void process(@DoFn.Element SequencedMessage seqMessage, MultiOutputReceiver receiver) {
      try {
        receiver
            .get(OUTPUT_TAG)
            .output(valueMapper.apply(seqMessage.getMessage().getData().toByteArray()));
      } catch (Exception e) {
        errorsInBundle += 1;
        LOG.warn("Error while parsing the element", e);
        receiver
            .get(ERROR_TAG)
            .output(
                Row.withSchema(ERROR_SCHEMA)
                    .addValues(e.toString(), seqMessage.getMessage().getData().toByteArray())
                    .build());
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      PubsubLiteReadSchemaTransformConfiguration configuration) {
    if (!VALID_DATA_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
    final Schema beamSchema =
        Objects.equals(configuration.getFormat(), "JSON")
            ? JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema())
            : AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(configuration.getSchema()));
    final SerializableFunction<byte[], Row> valueMapper =
        Objects.equals(configuration.getFormat(), "JSON")
            ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
            : AvroUtils.getAvroBytesToRowFunction(beamSchema);
    return new SchemaTransform() {
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
        PCollectionTuple outputTuple =
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
                                        SubscriptionName.of(configuration.getSubscriptionName()))
                                    .build())
                            .build()))
                .apply(
                    ParDo.of(new ErrorFn("PubsubLite-read-error-counter", valueMapper))
                        .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

        return PCollectionRowTuple.of(
            "output",
            outputTuple.get(OUTPUT_TAG).setRowSchema(beamSchema),
            "errors",
            outputTuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
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
    return Arrays.asList("output", "errors");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteReadSchemaTransformConfiguration {
    @SchemaFieldDescription(
        "The encoding format for the data stored in Pubsub Lite. Valid options are: "
            + VALID_FORMATS_STR)
    public abstract String getFormat();

    @SchemaFieldDescription(
        "The schema in which the data is encoded in the Kafka topic. "
            + "For AVRO data, this is a schema defined with AVRO schema syntax "
            + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas). "
            + "For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/).")
    public abstract String getSchema();

    @SchemaFieldDescription(
        "The GCP project where the Pubsub Lite reservation resides. This can be a "
            + "project number of a project ID.")
    public abstract @Nullable String getProject();

    @SchemaFieldDescription(
        "The name of the subscription to consume data. This will be concatenated with "
            + "the project and location parameters to build a full subscription path.")
    public abstract String getSubscriptionName();

    @SchemaFieldDescription("The region or zone where the Pubsub Lite reservation resides.")
    public abstract String getLocation();

    public static Builder builder() {
      return new AutoValue_PubsubLiteReadSchemaTransformProvider_PubsubLiteReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFormat(String format);

      public abstract Builder setSchema(String schema);

      public abstract Builder setProject(String project);

      public abstract Builder setSubscriptionName(String subscriptionName);

      public abstract Builder setLocation(String location);

      public abstract PubsubLiteReadSchemaTransformConfiguration build();
    }
  }
}
