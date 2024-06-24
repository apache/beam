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
package org.apache.beam.sdk.io.kafka;

import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslationTest;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class KafkaSchemaTransformTranslationTest {
  public static class ReadTranslationTest extends SchemaTransformTranslationTest {
    static final KafkaReadSchemaTransformProvider READ_PROVIDER =
        new KafkaReadSchemaTransformProvider();

    @Override
    protected SchemaTransformProvider provider() {
      return READ_PROVIDER;
    }

    @Override
    protected Row configurationRow() {
      return Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("format", "RAW")
          .withFieldValue("topic", "test_topic")
          .withFieldValue("bootstrap_servers", "host:port")
          .withFieldValue("confluent_schema_registry_url", null)
          .withFieldValue("confluent_schema_registry_subject", null)
          .withFieldValue("schema", null)
          .withFieldValue("file_descriptor_path", "testPath")
          .withFieldValue("message_name", "test_message")
          .withFieldValue("auto_offset_reset_config", "earliest")
          .withFieldValue("consumer_config_updates", ImmutableMap.<String, String>builder().build())
          .withFieldValue("error_handling", null)
          .build();
    }
  }

  public static class WriteTranslationTest extends SchemaTransformTranslationTest {
    static final KafkaWriteSchemaTransformProvider WRITE_PROVIDER =
        new KafkaWriteSchemaTransformProvider();

    @Override
    protected SchemaTransformProvider provider() {
      return WRITE_PROVIDER;
    }

    @Override
    protected Row configurationRow() {
      return Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("format", "RAW")
          .withFieldValue("topic", "test_topic")
          .withFieldValue("bootstrap_servers", "host:port")
          .withFieldValue("producer_config_updates", ImmutableMap.<String, String>builder().build())
          .withFieldValue("error_handling", null)
          .withFieldValue("file_descriptor_path", "testPath")
          .withFieldValue("message_name", "test_message")
          .withFieldValue("schema", "test_schema")
          .build();
    }

    @Override
    protected PCollectionRowTuple input(Pipeline p) {
      Schema inputSchema = Schema.builder().addByteArrayField("b").build();
      PCollection<Row> input =
          p.apply(
                  Create.of(
                      Collections.singletonList(
                          Row.withSchema(inputSchema).addValue(new byte[] {1, 2, 3}).build())))
              .setRowSchema(inputSchema);
      return PCollectionRowTuple.of("input", input);
    }
  }
}
