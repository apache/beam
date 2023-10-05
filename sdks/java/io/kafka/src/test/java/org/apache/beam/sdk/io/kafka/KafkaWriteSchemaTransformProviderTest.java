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
import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransform.ErrorCounterFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
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
public class KafkaWriteSchemaTransformProviderTest {

  private static final TupleTag<KV<byte[], byte[]>> OUTPUT_TAG =
      KafkaWriteSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = KafkaWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAMSCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));
  private static final Schema ERRORSCHEMA = KafkaWriteSchemaTransformProvider.ERROR_SCHEMA;

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "c").build());

  final SerializableFunction<Row, byte[]> valueMapper =
      JsonUtils.getRowToJsonBytesFunction(BEAMSCHEMA);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testKafkaErrorFnSuccess() throws Exception {
    List<KV<byte[], byte[]>> msg =
        Arrays.asList(
            KV.of(new byte[1], "{\"name\":\"a\"}".getBytes("UTF8")),
            KV.of(new byte[1], "{\"name\":\"b\"}".getBytes("UTF8")),
            KV.of(new byte[1], "{\"name\":\"c\"}".getBytes("UTF8")));

    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("Kafka-write-error-counter", valueMapper))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(ERRORSCHEMA);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(msg);
    p.run().waitUntilFinish();
  }
}
