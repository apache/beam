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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider.ErrorFn;
import org.apache.beam.sdk.schemas.Schema;
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
public class PubsubLiteDlqTest {

  private static final TupleTag<Row> OUTPUTTAG = PubsubLiteReadSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERRORTAG = PubsubLiteReadSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAMSCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));
  private static final Schema ERRORSCHEMA = PubsubLiteReadSchemaTransformProvider.ERROR_SCHEMA;

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "c").build());

  private static final List<SequencedMessage> MESSAGES =
      Arrays.asList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"a\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"b\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"c\"}"))
                      .build())
              .build());

  private static final List<SequencedMessage> MESSAGESWITHERROR =
      Arrays.asList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"a\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"b\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"c\"}"))
                      .build())
              .build());

  final SerializableFunction<byte[], Row> valueMapper =
      JsonUtils.getJsonBytesToRowFunction(BEAMSCHEMA);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPubsubLiteErrorFnSuccess() throws Exception {
    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(BEAMSCHEMA);
    output.get(ERRORTAG).setRowSchema(ERRORSCHEMA);

    PAssert.that(output.get(OUTPUTTAG)).containsInAnyOrder(ROWS);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnFailure() throws Exception {
    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGESWITHERROR));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper))
                .withOutputTags(OUTPUTTAG, TupleTagList.of(ERRORTAG)));

    output.get(OUTPUTTAG).setRowSchema(BEAMSCHEMA);
    output.get(ERRORTAG).setRowSchema(ERRORSCHEMA);

    PCollection<Long> count = output.get(ERRORTAG).apply("error_count", Count.globally());

    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));

    p.run().waitUntilFinish();
  }
}
