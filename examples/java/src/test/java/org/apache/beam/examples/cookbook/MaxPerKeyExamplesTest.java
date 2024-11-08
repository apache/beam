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
import java.util.List;
import org.apache.beam.examples.cookbook.MaxPerKeyExamples.ExtractTempFn;
import org.apache.beam.examples.cookbook.MaxPerKeyExamples.FormatMaxesFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MaxPerKeyExamples}. */
@RunWith(JUnit4.class)
public class MaxPerKeyExamplesTest {

  private static final TableRow row1 =
      new TableRow()
          .set("month", "6")
          .set("day", "21")
          .set("year", "2014")
          .set("mean_temp", "85.3")
          .set("tornado", true);
  private static final TableRow row2 =
      new TableRow()
          .set("month", "7")
          .set("day", "20")
          .set("year", "2014")
          .set("mean_temp", "75.4")
          .set("tornado", false);
  private static final TableRow row3 =
      new TableRow()
          .set("month", "6")
          .set("day", "18")
          .set("year", "2014")
          .set("mean_temp", "45.3")
          .set("tornado", true);
  private static final List<TableRow> TEST_ROWS = ImmutableList.of(row1, row2, row3);

  private static final KV<Integer, Double> kv1 = KV.of(6, 85.3);
  private static final KV<Integer, Double> kv2 = KV.of(6, 45.3);
  private static final KV<Integer, Double> kv3 = KV.of(7, 75.4);

  private static final List<KV<Integer, Double>> TEST_KVS = ImmutableList.of(kv1, kv2, kv3);

  private static final TableRow resultRow1 =
      new TableRow().set("month", 6).set("max_mean_temp", 85.3);
  private static final TableRow resultRow2 =
      new TableRow().set("month", 6).set("max_mean_temp", 45.3);
  private static final TableRow resultRow3 =
      new TableRow().set("month", 7).set("max_mean_temp", 75.4);

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testExtractTempFn() {
    PCollection<KV<Integer, Double>> results =
        p.apply(Create.of(TEST_ROWS)).apply(ParDo.of(new ExtractTempFn()));
    PAssert.that(results).containsInAnyOrder(ImmutableList.of(kv1, kv2, kv3));
    p.run().waitUntilFinish();
  }

  @Test
  public void testFormatMaxesFn() {
    PCollection<TableRow> results =
        p.apply(Create.of(TEST_KVS)).apply(ParDo.of(new FormatMaxesFn()));
    PAssert.that(results).containsInAnyOrder(resultRow1, resultRow2, resultRow3);
    p.run().waitUntilFinish();
  }
}
