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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
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

/** Tests for {@link MapToKeys} transform. */
@RunWith(JUnit4.class)
public class MapToKeysTest {

  private static final List<KV<Integer, String>> TABLE =
      new ArrayList<KV<Integer, String>>() {
        {
          add(KV.of(1, "one"));
          add(KV.of(2, "none"));
          add(KV.of(3, "none"));
        }
      };

  private static final List<KV<Integer, String>> EMPTY_TABLE = new ArrayList<>();

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testMapToKeys() {

    PCollection<KV<Integer, String>> input =
        p.apply(
            Create.of(TABLE)
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<Double> output =
        input.apply(MapToKeys.via(Integer::doubleValue)).setCoder(DoubleCoder.of());

    PAssert.that(output).containsInAnyOrder(1.0d, 2.0d, 3.0d);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMapToKeysEmpty() {

    PCollection<KV<Integer, String>> input =
        p.apply(
            Create.of(EMPTY_TABLE)
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of())));

    PCollection<Double> output =
        input.apply(MapToKeys.via(Integer::doubleValue)).setCoder(DoubleCoder.of());

    PAssert.that(output).empty();

    p.run();
  }
}
