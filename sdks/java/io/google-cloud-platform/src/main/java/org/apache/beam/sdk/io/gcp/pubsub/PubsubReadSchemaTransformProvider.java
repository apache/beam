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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.api.client.util.Clock;
import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
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

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Pub/Sub reads configured using
 * {@link PubsubReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class PubsubReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<PubsubReadSchemaTransformConfiguration> {

  public static final String VALID_FORMATS_STR = "AVRO,JSON";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final Schema ERROR_SCHEMA =
      Schema.builder().addStringField("error").addNullableByteArrayField("row").build();

  @Override
  public Class<PubsubReadSchemaTransformConfiguration> configurationClass() {
    return PubsubReadSchemaTransformConfiguration.class;
  }

  @Override
  public SchemaTransform from(PubsubReadSchemaTransformConfiguration configuration) {
    if (configuration.getSubscription() == null && configuration.getTopic() == null) {
      throw new IllegalArgumentException(
          "To read from Pubsub, a subscription name or a topic name must be provided");
    }

    if (configuration.getSubscription() != null && configuration.getTopic() != null) {
      throw new IllegalArgumentException(
          "To read from Pubsub, a subscription name or a topic name must be provided. Not both.");
    }

    if ((Strings.isNullOrEmpty(configuration.getSchema())
            && !Strings.isNullOrEmpty(configuration.getFormat()))
        || (!Strings.isNullOrEmpty(configuration.getSchema())
            && Strings.isNullOrEmpty(configuration.getFormat()))) {
      throw new IllegalArgumentException(
          "A schema was provided without a data format (or viceversa). Please provide "
              + "both of these parameters to read from Pubsub, or if you would like to use the Pubsub schema service,"
              + " please leave both of these blank.");
    }

    Schema beamSchema;
    SerializableFunction<byte[], Row> valueMapper;

    if (!VALID_DATA_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
    beamSchema =
        Objects.equals(configuration.getFormat(), "JSON")
            ? JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema())
            : AvroUtils.toBeamSchema(
                new org.apache.avro.Schema.Parser().parse(configuration.getSchema()));
    valueMapper =
        Objects.equals(configuration.getFormat(), "JSON")
            ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
            : AvroUtils.getAvroBytesToRowFunction(beamSchema);

    PubsubReadSchemaTransform transform =
        new PubsubReadSchemaTransform(
            configuration.getTopic(), configuration.getSubscription(), beamSchema, valueMapper);

    if (configuration.getClientFactory() != null) {
      transform.setClientFactory(configuration.getClientFactory());
    }
    if (configuration.getClock() != null) {
      transform.setClock(configuration.getClock());
    }

    return transform;
  }

  private static class PubsubReadSchemaTransform extends SchemaTransform implements Serializable {
    final Schema beamSchema;
    final SerializableFunction<byte[], Row> valueMapper;
    final @Nullable String topic;
    final @Nullable String subscription;
    @Nullable PubsubTestClientFactory clientFactory;
    @Nullable Clock clock;

    PubsubReadSchemaTransform(
        @Nullable String topic,
        @Nullable String subscription,
        Schema beamSchema,
        SerializableFunction<byte[], Row> valueMapper) {
      this.topic = topic;
      this.subscription = subscription;
      this.beamSchema = beamSchema;
      this.valueMapper = valueMapper;
    }

    private static class ErrorCounterFn extends DoFn<PubsubMessage, Row> {
      private Counter pubsubErrorCounter;
      private Long errorsInBundle = 0L;
      private SerializableFunction<byte[], Row> valueMapper;

      ErrorCounterFn(String name, SerializableFunction<byte[], Row> valueMapper) {
        this.pubsubErrorCounter = Metrics.counter(PubsubReadSchemaTransformProvider.class, name);
        this.valueMapper = valueMapper;
      }

      @ProcessElement
      public void process(@DoFn.Element PubsubMessage message, MultiOutputReceiver receiver) {

        try {
          receiver.get(OUTPUT_TAG).output(valueMapper.apply(message.getPayload()));
        } catch (Exception e) {
          errorsInBundle += 1;
          receiver
              .get(ERROR_TAG)
              .output(
                  Row.withSchema(ERROR_SCHEMA)
                      .addValues(e.toString(), message.getPayload())
                      .build());
        }
      }

      @FinishBundle
      public void finish(FinishBundleContext c) {
        pubsubErrorCounter.inc(errorsInBundle);
        errorsInBundle = 0L;
      }
    }

    void setClientFactory(@Nullable PubsubTestClientFactory factory) {
      this.clientFactory = factory;
    }

    void setClock(@Nullable Clock clock) {
      this.clock = clock;
    }

    @SuppressWarnings("nullness")
    PubsubIO.Read<PubsubMessage> buildPubsubRead() {
      PubsubIO.Read<PubsubMessage> pubsubRead = PubsubIO.readMessages();
      if (!Strings.isNullOrEmpty(topic)) {
        pubsubRead = pubsubRead.fromTopic(topic);
      } else {
        pubsubRead = pubsubRead.fromSubscription(subscription);
      }
      if (clientFactory != null && clock != null) {
        pubsubRead = pubsubRead.withClientFactory(clientFactory);
        pubsubRead = clientFactory.setClock(pubsubRead, clock);
      } else if (clientFactory != null || clock != null) {
        throw new IllegalArgumentException(
            "Both PubsubTestClientFactory and Clock need to be specified for testing, but only one is provided");
      }
      return pubsubRead;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PubsubIO.Read<PubsubMessage> pubsubRead = buildPubsubRead();

      PCollectionTuple outputTuple =
          input
              .getPipeline()
              .apply(pubsubRead)
              .apply(
                  ParDo.of(new ErrorCounterFn("PubSub-read-error-counter", valueMapper))
                      .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      return PCollectionRowTuple.of(
          "output",
          outputTuple.get(OUTPUT_TAG).setRowSchema(beamSchema),
          "errors",
          outputTuple.get(ERROR_TAG).setRowSchema(ERROR_SCHEMA));
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:pubsub_read:v1";
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
}
