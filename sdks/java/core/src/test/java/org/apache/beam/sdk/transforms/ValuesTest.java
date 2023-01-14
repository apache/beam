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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Values transform. */
@RunWith(JUnit4.class)
public class ValuesTest {
  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  static final KV<String, Integer>[] TABLE =
      new KV[] {
        KV.of("one", 1), KV.of("two", 2), KV.of("three", 3), KV.of("four", 4), KV.of("dup", 4)
      };

  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {};

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testValues() {

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(Arrays.asList(TABLE))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<Integer> output = input.apply(Values.create());

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4, 4);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testValuesEmpty() {

    PCollection<KV<String, Integer>> input =
        p.apply(
            Create.of(Arrays.asList(EMPTY_TABLE))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));

    PCollection<Integer> output = input.apply(Values.create());

    PAssert.that(output).empty();

    p.run();
  }

  @Test
  public void testValuesGetName() {
    assertEquals("Values", Values.<Integer>create().getName());
  }
}
