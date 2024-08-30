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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaReadSchemaTransformProvider.ErrorFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaDlqTest {

  private static final TupleTag<Row> OUTPUTTAG = KafkaReadSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERRORTAG = KafkaReadSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAMSCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "c").build());

  private static List<byte[]> messages;

  private static List<byte[]> messagesWithError;

  final SerializableFunction<byte[], Row> valueMapper =
      JsonUtils.getJsonBytesToRowFunction(BEAMSCHEMA);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testKafkaErrorFnSuccess() throws Exception {
    try {
      messages =
          Arrays.asList(
              "{\"name\":\"a\"}".getBytes("UTF8"),
              "{\"name\":\"b\"}".getBytes("UTF8"),
              "{\"name\":\"c\"}".getBytes("UTF8"));
    } catch (Exception e) {
    }
    PCollection<byte[]> input = p.apply(Create.of(messages));
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Kafka-read-error-counter", valueMapper, errorSchema, true))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(BEAMSCHEMA);
    output.get(ERRORTAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUTTAG)).containsInAnyOrder(ROWS);
    p.run().waitUntilFinish();
  }

  @Test
  public void testKafkaErrorFnFailure() throws Exception {
    try {
      messagesWithError =
          Arrays.asList(
              "{\"error\":\"a\"}".getBytes("UTF8"),
              "{\"error\":\"b\"}".getBytes("UTF8"),
              "{\"error\":\"c\"}".getBytes("UTF8"));
    } catch (Exception e) {
    }
    PCollection<byte[]> input = p.apply(Create.of(messagesWithError));
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper, errorSchema, true))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(BEAMSCHEMA);
    output.get(ERRORTAG).setRowSchema(errorSchema);

    PCollection<Long> count = output.get(ERRORTAG).apply("error_count", Count.globally());

    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));

    p.run().waitUntilFinish();
  }
}
