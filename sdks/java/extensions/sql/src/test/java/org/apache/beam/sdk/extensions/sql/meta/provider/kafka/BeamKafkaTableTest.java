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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/** Test utility for BeamKafkaTable implementations. */
public abstract class BeamKafkaTableTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  /** Returns proper implementation of BeamKafkaTable for the tested format. */
  protected abstract BeamKafkaTable getBeamKafkaTable();

  /** Returns encoded payload in the tested format corresponding to the row in `generateRow(i)`. */
  protected abstract byte[] generateEncodedPayload(int i);

  /** Provides a deterministic row from the given integer. */
  protected abstract Row generateRow(int i);

  @Test
  public void testRecorderDecoder() {
    BeamKafkaTable kafkaTable = getBeamKafkaTable();

    PCollection<Row> result =
        pipeline
            .apply(Create.of(generateEncodedPayload(1), generateEncodedPayload(2)))
            .apply(MapElements.via(new ToKV()))
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
            .apply(kafkaTable.getPTransformForInput());
    PAssert.that(result).containsInAnyOrder(generateRow(1), generateRow(2));
    pipeline.run();
  }

  private static class ToKV extends SimpleFunction<byte[], KV<byte[], byte[]>> {
    @Override
    public KV<byte[], byte[]> apply(byte[] bytes) {
      return KV.of(new byte[] {}, bytes);
    }
  }
}
