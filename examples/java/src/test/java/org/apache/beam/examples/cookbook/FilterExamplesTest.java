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
import org.apache.beam.examples.cookbook.FilterExamples.FilterSingleMonthDataFn;
import org.apache.beam.examples.cookbook.FilterExamples.ProjectionFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FilterExamples}. */
@RunWith(JUnit4.class)
public class FilterExamplesTest {

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

  private static final TableRow outRow1 =
      new TableRow().set("year", 2014).set("month", 6).set("day", 21).set("mean_temp", 85.3);
  private static final TableRow outRow2 =
      new TableRow().set("year", 2014).set("month", 7).set("day", 20).set("mean_temp", 75.4);
  private static final TableRow outRow3 =
      new TableRow().set("year", 2014).set("month", 6).set("day", 18).set("mean_temp", 45.3);

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testProjectionFn() {
    PCollection<TableRow> input = p.apply(Create.of(row1, row2, row3));

    PCollection<TableRow> results = input.apply(ParDo.of(new ProjectionFn()));

    PAssert.that(results).containsInAnyOrder(outRow1, outRow2, outRow3);
    p.run().waitUntilFinish();
  }

  @Test
  public void testFilterSingleMonthDataFn() {
    PCollection<TableRow> input = p.apply(Create.of(outRow1, outRow2, outRow3));

    PCollection<TableRow> results = input.apply(ParDo.of(new FilterSingleMonthDataFn(7)));

    PAssert.that(results).containsInAnyOrder(outRow2);
    p.run().waitUntilFinish();
  }
}
