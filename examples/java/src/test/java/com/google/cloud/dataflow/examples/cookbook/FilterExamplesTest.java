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
import com.google.cloud.dataflow.examples.cookbook.FilterExamples.FilterSingleMonthDataFn;
import com.google.cloud.dataflow.examples.cookbook.FilterExamples.ProjectionFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link FilterExamples}. */
@RunWith(JUnit4.class)
public class FilterExamplesTest {

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
  static final TableRow[] ROWS_ARRAY = new TableRow[] {
    row1, row2, row3
  };
  static final List<TableRow> ROWS = Arrays.asList(ROWS_ARRAY);

  private static final TableRow outRow1 = new TableRow()
      .set("year", 2014).set("month", 6)
      .set("day", 21).set("mean_temp", 85.3);
  private static final TableRow outRow2 = new TableRow()
      .set("year", 2014).set("month", 7)
      .set("day", 20).set("mean_temp", 75.4);
  private static final TableRow outRow3 = new TableRow()
      .set("year", 2014).set("month", 6)
      .set("day", 18).set("mean_temp", 45.3);
  private static final TableRow[] PROJROWS_ARRAY = new TableRow[] {
    outRow1, outRow2, outRow3
  };


  @Test
  public void testProjectionFn() {
    DoFnTester<TableRow, TableRow> projectionFn =
        DoFnTester.of(new ProjectionFn());
    List<TableRow> results = projectionFn.processBatch(ROWS_ARRAY);
    Assert.assertThat(results, CoreMatchers.hasItem(outRow1));
    Assert.assertThat(results, CoreMatchers.hasItem(outRow2));
    Assert.assertThat(results, CoreMatchers.hasItem(outRow3));
  }

  @Test
  public void testFilterSingleMonthDataFn() {
    DoFnTester<TableRow, TableRow> filterSingleMonthDataFn =
        DoFnTester.of(new FilterSingleMonthDataFn(7));
    List<TableRow> results = filterSingleMonthDataFn.processBatch(PROJROWS_ARRAY);
    Assert.assertThat(results, CoreMatchers.hasItem(outRow2));
  }
}
