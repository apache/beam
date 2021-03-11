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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Rule;
import org.junit.Test;

/** Test utility for BeamKafkaTable implementations. */
public abstract class BeamKafkaTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /** Returns proper implementation of BeamKafkaTable for the tested format. */
  protected abstract BeamKafkaTable getBeamKafkaTable();

  /** Returns encoded payload in the tested format corresponding to the row in `generateRow(i)`. */
  protected abstract byte[] generateEncodedPayload(int i) throws Exception;

  /** Provides a deterministic row from the given integer. */
  protected abstract Row generateRow(int i);

  @Test
  public void testRecorderDecoder() throws Exception {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();

    PCollection<Row> result =
        pipeline
            .apply(Create.of(generateEncodedPayload(1), generateEncodedPayload(2)))
            .apply(MapElements.via(new BytesToRecord()))
            .setCoder(KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
            .apply(kafkaTable.getPTransformForInput());

    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  @Test
  public void testRecorderEncoder() {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();
    PCollection<Row> result =
        pipeline
            .apply(Create.of(generateRow(1), generateRow(2)))
            .apply(kafkaTable.getPTransformForOutput())
            .setCoder(ProducerRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
            .apply(MapElements.via(new ProducerToRecord()))
            .setCoder(KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
            .apply(kafkaTable.getPTransformForInput());
    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  private static class BytesToRecord extends SimpleFunction<byte[], KafkaRecord<byte[], byte[]>> {
    @Override
    public KafkaRecord<byte[], byte[]> apply(byte[] bytes) {
      return new KafkaRecord<>(
          "abc",
          0,
          0,
          0,
          KafkaTimestampType.LOG_APPEND_TIME,
          new RecordHeaders(),
          KV.of(new byte[] {}, bytes));
    }
  }

  static class ProducerToRecord
      extends SimpleFunction<ProducerRecord<byte[], byte[]>, KafkaRecord<byte[], byte[]>> {
    @Override
    public KafkaRecord<byte[], byte[]> apply(ProducerRecord<byte[], byte[]> record) {
      return new KafkaRecord<>(
          record.topic(),
          record.partition() != null ? record.partition() : 0,
          0,
          0,
          KafkaTimestampType.LOG_APPEND_TIME,
          record.headers(),
          record.key(),
          record.value());
    }
  }
}
