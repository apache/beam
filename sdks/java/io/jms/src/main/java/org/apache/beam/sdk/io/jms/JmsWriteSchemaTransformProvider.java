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
import static org.apache.beam.sdk.io.jms.JmsWriteSchemaTransformProvider.WriteConfiguration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
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

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JmsWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<WriteConfiguration> {
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class WriteConfiguration implements Serializable {
    public static Builder builder() {
      return new AutoValue_JmsWriteSchemaTransformProvider_WriteConfiguration.Builder();
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
        "The JMS queue to write to. Exclusively one of queue or topic must be specified.")
    @Nullable
    public abstract String getQueue();

    @SchemaFieldDescription(
        "The JMS topic to write to. Exclusively one of queue or topic must be specified.")
    @Nullable
    public abstract String getTopic();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setConnectionConfiguration(
          ConnectionConfiguration connectionConfiguration);

      public abstract Builder setQueue(String queue);

      public abstract Builder setTopic(String topic);

      public abstract WriteConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:jms_write:v1";
  }

  @Override
  public String description() {
    return "Publishes messages to a JMS broker. Expects an input PCollection of rows with a "
        + "`payload` (string) or `bytes` field, each of which is published as one JMS TextMessage.\n"
        + "\n"
        + "Works with both bounded (batch) and unbounded (streaming) input PCollections.";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  protected SchemaTransform from(WriteConfiguration configuration) {
    return new JmsWriteSchemaTransform(configuration);
  }

  private static class JmsWriteSchemaTransform extends SchemaTransform {
    private final WriteConfiguration config;

    JmsWriteSchemaTransform(WriteConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.getSinglePCollection();

      Preconditions.checkArgument(
          (config.getQueue() != null) != (config.getTopic() != null),
          "Exactly one of queue or topic must be specified.");

      int fieldIndex = -1;
      boolean isBytes = false;
      Schema schema = inputRows.getSchema();
      if (schema.hasField("payload")
          && schema.getField("payload").getType().equals(Schema.FieldType.STRING)) {
        fieldIndex = schema.indexOf("payload");
      } else if (schema.hasField("bytes")
          && schema.getField("bytes").getType().equals(Schema.FieldType.BYTES)) {
        fieldIndex = schema.indexOf("bytes");
        isBytes = true;
      } else if (schema.getFieldCount() == 1
          && schema.getField(0).getType().equals(Schema.FieldType.STRING)) {
        fieldIndex = 0;
      } else if (schema.getFieldCount() == 1
          && schema.getField(0).getType().equals(Schema.FieldType.BYTES)) {
        fieldIndex = 0;
        isBytes = true;
      } else {
        throw new IllegalStateException(
            String.format(
                "Expected input Schema to have a 'payload' (STRING) or 'bytes' (BYTES) field, or"
                    + " a single string/bytes field, but received: %s",
                schema));
      }

      JmsIO.Write<String> writeTransform =
          JmsIO.<String>write()
              .withConnectionConfiguration(config.getConnectionConfiguration())
              .withValueMapper(new TextMessageMapper());

      String queue = config.getQueue();
      if (queue != null) {
        writeTransform = writeTransform.withQueue(queue);
      }
      String topic = config.getTopic();
      if (topic != null) {
        writeTransform = writeTransform.withTopic(topic);
      }

      final int idx = fieldIndex;
      final boolean bytesField = isBytes;
      inputRows
          .apply(
              "Extract payload",
              ParDo.of(
                  new DoFn<Row, String>() {
                    @ProcessElement
                    public void processElement(
                        @Element Row row, OutputReceiver<String> outputReceiver) {
                      if (bytesField) {
                        byte[] bytes =
                            org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
                                row.getBytes(idx));
                        outputReceiver.output(new String(bytes, StandardCharsets.UTF_8));
                      } else {
                        String payload =
                            org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
                                row.getString(idx));
                        outputReceiver.output(payload);
                      }
                    }
                  }))
          .apply(writeTransform);

      return PCollectionRowTuple.empty(inputRows.getPipeline());
    }
  }
}
