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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.sdk.io.jms.JmsIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.jms.JmsReadSchemaTransformProvider.ReadConfiguration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JmsReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<ReadConfiguration> {
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class ReadConfiguration implements Serializable {
    public static Builder builder() {
      return new AutoValue_JmsReadSchemaTransformProvider_ReadConfiguration.Builder();
    }

    @SchemaFieldDescription(
        "Configuration options to set up the JMS connection.\nNote: if "
            + "connection factory class name is set, spin up a persistent expansion service with "
            + "the provider client JAR on the classpath:\n"
            + "java -cp <expansion-service-jar>:<provider-client-jar>\n"
            + "org.apache.beam.sdk.expansion.service.ExpansionService <port>\n"
            + "and pass expansion_service='localhost:<port>' to the transform. Currently, only "
            + "ActiveMQ (org.apache.activemq.ActiveMQConnectionFactory) is embedded into the expansion service")
    public abstract ConnectionConfiguration getConnectionConfiguration();

    @SchemaFieldDescription(
        "The JMS queue to read from. Exclusively one of queue or topic must be specified.")
    @Nullable
    public abstract String getQueue();

    @SchemaFieldDescription(
        "The JMS topic to read from. Exclusively one of queue or topic must be specified.")
    @Nullable
    public abstract String getTopic();

    @SchemaFieldDescription(
        "The max number of records to receive. Setting this will result in a bounded PCollection.")
    @Nullable
    public abstract Long getMaxNumRecords();

    @SchemaFieldDescription(
        "The maximum time for this source to read messages. Setting this will result in a bounded PCollection.")
    @Nullable
    public abstract Long getMaxReadTimeSeconds();

    @SchemaFieldDescription("Close timeout for the JMS connection in seconds.")
    @Nullable
    public abstract Long getCloseTimeoutSeconds();

    @SchemaFieldDescription(
        "The JMS acknowledge mode: CLIENT_ACKNOWLEDGE, CLIENT_ACKNOWLEDGE_UNSAFE, or INDIVIDUAL_ACKNOWLEDGE.")
    @Nullable
    public abstract String getAcknowledgeMode();

    @SchemaFieldDescription(
        "The proprietary integer code for individual message acknowledgment when using INDIVIDUAL_ACKNOWLEDGE.")
    @Nullable
    public abstract Integer getIndividualAcknowledgeModeCode();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      public abstract Builder setQueue(String queue);

      public abstract Builder setTopic(String topic);

      public abstract Builder setMaxNumRecords(Long maxNumRecords);

      public abstract Builder setMaxReadTimeSeconds(Long maxReadTimeSeconds);

      public abstract Builder setCloseTimeoutSeconds(Long closeTimeoutSeconds);

      public abstract Builder setAcknowledgeMode(String acknowledgeMode);

      public abstract Builder setIndividualAcknowledgeModeCode(
          Integer individualAcknowledgeModeCode);

      public abstract ReadConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:jms_read:v1";
  }

  @Override
  public String description() {
    return "Reads messages from a JMS broker and outputs each message as a single `payload` "
        + "(string) field.\n"
        + "\n"
        + "By default the read is unbounded (streaming): it keeps consuming messages from the "
        + "specified queue or topic until the pipeline is stopped. Setting `maxNumRecords` and/or "
        + "`maxReadTimeSeconds` bounds the read, producing a bounded (batch) PCollection.";
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @Override
  protected SchemaTransform from(ReadConfiguration configuration) {
    return new JmsReadSchemaTransform(configuration);
  }

  private static class JmsReadSchemaTransform extends SchemaTransform {
    private final ReadConfiguration config;

    JmsReadSchemaTransform(ReadConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Preconditions.checkState(
          input.getAll().isEmpty(),
          "Expected zero input PCollections for this source, but found: %s",
          input.getAll().keySet());

      Preconditions.checkArgument(
          (config.getQueue() != null) != (config.getTopic() != null),
          "Exactly one of queue or topic must be specified.");

      JmsIO.Read<JmsRecord> readTransform =
          JmsIO.read().withConnectionConfiguration(config.getConnectionConfiguration());

      String queue = config.getQueue();
      if (queue != null) {
        readTransform = readTransform.withQueue(queue);
      }
      String topic = config.getTopic();
      if (topic != null) {
        readTransform = readTransform.withTopic(topic);
      }

      Long maxRecords = config.getMaxNumRecords();
      Long maxReadTime = config.getMaxReadTimeSeconds();
      if (maxRecords != null) {
        readTransform = readTransform.withMaxNumRecords(maxRecords);
      }
      if (maxReadTime != null) {
        readTransform = readTransform.withMaxReadTime(Duration.standardSeconds(maxReadTime));
      }

      Long closeTimeout = config.getCloseTimeoutSeconds();
      if (closeTimeout != null) {
        readTransform = readTransform.withCloseTimeout(Duration.standardSeconds(closeTimeout));
      }

      String ackMode = config.getAcknowledgeMode();
      if (ackMode != null) {
        readTransform =
            readTransform.withAcknowledgeMode(JmsIO.AcknowledgeMode.valueOf(ackMode.toUpperCase()));
      }

      Integer indAckCode = config.getIndividualAcknowledgeModeCode();
      if (indAckCode != null) {
        readTransform = readTransform.withIndividualAcknowledgeModeCode(indAckCode);
      }

      Schema outputSchema = Schema.builder().addStringField("payload").build();

      PCollection<Row> outputRows =
          input
              .getPipeline()
              .apply(readTransform)
              .apply(
                  "Wrap in Beam Rows",
                  ParDo.of(
                      new DoFn<JmsRecord, Row>() {
                        @ProcessElement
                        public void processElement(
                            @Element JmsRecord record, OutputReceiver<Row> outputReceiver) {
                          String payload = record.getPayload();
                          if (payload == null) {
                            payload = "";
                          }
                          outputReceiver.output(
                              Row.withSchema(outputSchema).addValue(payload).build());
                        }
                      }))
              .setRowSchema(outputSchema);

      return PCollectionRowTuple.of("output", outputRows);
    }
  }
}
