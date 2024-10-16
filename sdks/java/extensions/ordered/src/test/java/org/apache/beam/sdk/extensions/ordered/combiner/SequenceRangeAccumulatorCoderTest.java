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
package org.apache.beam.sdk.extensions.ordered.combiner;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.extensions.ordered.combiner.SequenceRangeAccumulator.SequenceRangeAccumulatorCoder;
import org.joda.time.Instant;
import org.junit.Test;

public class SequenceRangeAccumulatorCoderTest {

  private SequenceRangeAccumulatorCoder coder = SequenceRangeAccumulatorCoder.of();

  @Test
  public void testEncodingEmptyAccumulator() throws IOException {
    SequenceRangeAccumulator empty = new SequenceRangeAccumulator();

    doTestEncodingAndDecoding(empty);
  }

  @Test
  public void testEncodingAccumulatorWithoutInitialSequence() throws IOException {
    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    accumulator.add(1, Instant.now(), false);
    accumulator.add(2, Instant.now(), false);
    accumulator.add(3, Instant.now(), false);
    accumulator.add(5, Instant.now(), false);
    accumulator.add(6, Instant.now(), false);

    doTestEncodingAndDecoding(accumulator);
  }

  @Test
  public void testEncodingAccumulatorWithInitialSequence() throws IOException {
    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    accumulator.add(1, Instant.now(), true);
    accumulator.add(2, Instant.now(), false);
    accumulator.add(3, Instant.now(), false);
    accumulator.add(5, Instant.now(), false);
    accumulator.add(6, Instant.now(), false);

    doTestEncodingAndDecoding(accumulator);
  }

  private void doTestEncodingAndDecoding(SequenceRangeAccumulator value) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(value, output);

    SequenceRangeAccumulator decoded = coder.decode(new ByteArrayInputStream(output.toByteArray()));
    assertEquals("Accumulator", value, decoded);
  }
}
