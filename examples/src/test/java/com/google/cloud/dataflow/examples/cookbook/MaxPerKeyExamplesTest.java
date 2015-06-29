/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.cookbook;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.examples.cookbook.MaxPerKeyExamples.ExtractTempFn;
import com.google.cloud.dataflow.examples.cookbook.MaxPerKeyExamples.FormatMaxesFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/** Unit tests for {@link MaxPerKeyExamples}. */
@RunWith(JUnit4.class)
public class MaxPerKeyExamplesTest {

  private static final TableRow row1 = new TableRow()
        .set("month", "6").set("day", "21")
        .set("year", "2014").set("mean_temp", "85.3")
        .set("tornado", true);
  private static final TableRow row2 = new TableRow()
        .set("month", "7").set("day", "20")
        .set("year", "2014").set("mean_temp", "75.4")
        .set("tornado", false);
  private static final TableRow row3 = new TableRow()
        .set("month", "6").set("day", "18")
        .set("year", "2014").set("mean_temp", "45.3")
        .set("tornado", true);
  private static final TableRow[] ROWS_ARRAY = new TableRow[] {
    row1, row2, row3
  };

  private static final KV<Integer, Double> kv1 = KV.of(6, 85.3);
  private static final KV<Integer, Double> kv2 = KV.of(6, 45.3);
  private static final KV<Integer, Double> kv3 = KV.of(7, 75.4);

  static final KV[] TUPLES_ARRAY = new KV[] {
    kv1, kv2, kv3
  };

  private static final TableRow resultRow1 = new TableRow()
      .set("month", 6)
      .set("max_mean_temp", 85.3);
  private static final TableRow resultRow2 = new TableRow()
      .set("month", 7)
      .set("max_mean_temp", 75.4);


  @Test
  public void testExtractTempFn() {
    DoFnTester<TableRow, KV<Integer, Double>> extractTempFn =
        DoFnTester.of(new ExtractTempFn());
    List<KV<Integer, Double>> results = extractTempFn.processBatch(ROWS_ARRAY);
    Assert.assertThat(results, CoreMatchers.hasItem(kv1));
    Assert.assertThat(results, CoreMatchers.hasItem(kv2));
    Assert.assertThat(results, CoreMatchers.hasItem(kv3));
  }

  @Test
  public void testFormatMaxesFn() {
    DoFnTester<KV<Integer, Double>, TableRow> formatMaxesFnFn =
        DoFnTester.of(new FormatMaxesFn());
    List<TableRow> results = formatMaxesFnFn.processBatch(TUPLES_ARRAY);
    Assert.assertThat(results, CoreMatchers.hasItem(resultRow1));
    Assert.assertThat(results, CoreMatchers.hasItem(resultRow2));
  }

}
