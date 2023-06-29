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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.MAIN_TAG;

import com.google.api.client.util.Clock;
import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Pub/Sub reads configured using
 * {@link PubsubReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
@AutoService(SchemaTransformProvider.class)
public class PubsubReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<PubsubReadSchemaTransformConfiguration> {
  static final String OUTPUT_TAG = "OUTPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<PubsubReadSchemaTransformConfiguration> configurationClass() {
    return PubsubReadSchemaTransformConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(PubsubReadSchemaTransformConfiguration configuration) {
    PubsubMessageToRow toRowTransform =
        PubsubSchemaTransformMessageToRowFactory.from(configuration).buildMessageToRow();
    return new PubsubReadSchemaTransform(configuration, toRowTransform);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:pubsub_read:v1";
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since
   * no input is expected, this returns an empty list.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * a single output is expected, this returns a list with a single name.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /**
   * An implementation of {@link SchemaTransform} for Pub/Sub reads configured using {@link
   * PubsubReadSchemaTransformConfiguration}.
   */
  static class PubsubReadSchemaTransform extends SchemaTransform {

    private final PubsubReadSchemaTransformConfiguration configuration;
    private final PubsubMessageToRow pubsubMessageToRow;

    private PubsubClient.PubsubClientFactory clientFactory;

    private Clock clock;

    private PubsubReadSchemaTransform(
        PubsubReadSchemaTransformConfiguration configuration,
        PubsubMessageToRow pubsubMessageToRow) {
      this.configuration = configuration;
      this.pubsubMessageToRow = pubsubMessageToRow;
    }

    /**
     * Sets the {@link PubsubClient.PubsubClientFactory}.
     *
     * <p>Used for testing.
     */
    void setClientFactory(PubsubClient.PubsubClientFactory value) {
      this.clientFactory = value;
    }

    /**
     * Sets the {@link Clock}.
     *
     * <p>Used for testing.
     */
    void setClock(Clock clock) {
      this.clock = clock;
    }

    /** Validates the {@link PubsubReadSchemaTransformConfiguration}. */
    @Override
    public void validate(@Nullable PipelineOptions options) {
      if (configuration.getSubscription() == null && configuration.getTopic() == null) {
        throw new IllegalArgumentException(
            String.format(
                "%s needs to set either the topic or the subscription",
                PubsubReadSchemaTransformConfiguration.class));
      }

      if (configuration.getSubscription() != null && configuration.getTopic() != null) {
        throw new IllegalArgumentException(
            String.format(
                "%s should not set both the topic or the subscription",
                PubsubReadSchemaTransformConfiguration.class));
      }

      try {
        PayloadSerializers.getSerializer(
            configuration.getFormat(), configuration.getDataSchema(), new HashMap<>());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid %s, no serializer provider exists for format `%s`",
                PubsubReadSchemaTransformConfiguration.class, configuration.getFormat()));
      }
    }

    /** Reads from Pub/Sub according to {@link PubsubReadSchemaTransformConfiguration}. */
    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.getAll().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s input is expected to be empty",
                input.getClass().getSimpleName(), getClass().getSimpleName()));
      }

      PCollectionTuple rowsWithDlq =
          input
              .getPipeline()
              .apply("ReadFromPubsub", buildPubsubRead())
              .apply("PubsubMessageToRow", pubsubMessageToRow);

      writeToDeadLetterQueue(rowsWithDlq);

      return PCollectionRowTuple.of(OUTPUT_TAG, rowsWithDlq.get(MAIN_TAG));
    }

    private void writeToDeadLetterQueue(PCollectionTuple rowsWithDlq) {
      PubsubIO.Write<PubsubMessage> deadLetterQueue = buildDeadLetterQueueWrite();
      if (deadLetterQueue == null) {
        return;
      }
      rowsWithDlq.get(DLQ_TAG).apply("WriteToDeadLetterQueue", deadLetterQueue);
    }

    /**
     * Builds {@link PubsubIO.Write} dead letter queue from {@link
     * PubsubReadSchemaTransformConfiguration}.
     */
    PubsubIO.Write<PubsubMessage> buildDeadLetterQueueWrite() {
      if (configuration.getDeadLetterQueue() == null) {
        return null;
      }

      PubsubIO.Write<PubsubMessage> writeDlq =
          PubsubIO.writeMessages().to(configuration.getDeadLetterQueue());

      if (configuration.getTimestampAttribute() != null) {
        writeDlq = writeDlq.withTimestampAttribute(configuration.getTimestampAttribute());
      }

      return writeDlq;
    }

    /** Builds {@link PubsubIO.Read} from a {@link PubsubReadSchemaTransformConfiguration}. */
    PubsubIO.Read<PubsubMessage> buildPubsubRead() {
      PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributes();

      if (configuration.getSubscription() != null) {
        read = read.fromSubscription(configuration.getSubscription());
      }

      if (configuration.getTopic() != null) {
        read = read.fromTopic(configuration.getTopic());
      }

      if (configuration.getTimestampAttribute() != null) {
        read = read.withTimestampAttribute(configuration.getTimestampAttribute());
      }

      if (configuration.getIdAttribute() != null) {
        read = read.withIdAttribute(configuration.getIdAttribute());
      }

      if (clientFactory != null) {
        read = read.withClientFactory(clientFactory);
      }

      if (clock != null) {
        read = read.withClock(clock);
      }

      return read;
    }
  }
}
