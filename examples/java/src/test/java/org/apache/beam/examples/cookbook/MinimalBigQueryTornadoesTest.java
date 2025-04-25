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
package org.apache.beam.examples.cookbook;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.examples.cookbook.MinimalBigQueryTornadoes.ExtractTornadoesFn;
import org.apache.beam.examples.cookbook.MinimalBigQueryTornadoes.FormatCountsFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link MinimalBigQueryTornadoes}. */
@RunWith(JUnit4.class)
public class MinimalBigQueryTornadoesTest {
  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testExtractTornadoes() {
    TableRow row = new TableRow().set("month", "6").set("tornado", true);
    PCollection<TableRow> input = p.apply(Create.of(ImmutableList.of(row)));
    PCollection<Integer> result = input.apply(ParDo.of(new ExtractTornadoesFn()));
    PAssert.that(result).containsInAnyOrder(6);
    p.run().waitUntilFinish();
  }

  @Test
  public void testNoTornadoes() {
    TableRow row = new TableRow().set("month", 6).set("tornado", false);
    PCollection<TableRow> inputs = p.apply(Create.of(ImmutableList.of(row)));
    PCollection<Integer> result = inputs.apply(ParDo.of(new ExtractTornadoesFn()));
    PAssert.that(result).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void testEmpty() {
    PCollection<KV<Integer, Long>> inputs =
        p.apply(Create.empty(new TypeDescriptor<KV<Integer, Long>>() {}));
    PCollection<String> result = inputs.apply(ParDo.of(new FormatCountsFn()));
    PAssert.that(result).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void testFormatCounts() {
    PCollection<KV<Integer, Long>> inputs =
        p.apply(Create.of(KV.of(3, 0L), KV.of(4, Long.MAX_VALUE), KV.of(5, Long.MIN_VALUE)));
    PCollection<String> result = inputs.apply(ParDo.of(new FormatCountsFn()));
    PAssert.that(result).containsInAnyOrder("3: 0", "4: " + Long.MAX_VALUE, "5: " + Long.MIN_VALUE);
    p.run().waitUntilFinish();
  }
}
