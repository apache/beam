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
package org.apache.beam.runners.dataflow.io;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.transforms.DataflowDisplayDataEvaluator;
import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;

import com.google.api.services.bigquery.model.TableSchema;

import org.junit.Test;

import java.util.Set;

/**
 * Unit tests for Dataflow usage of {@link BigQueryIO} transforms.
 */
public class DataflowBigQueryIOTest {
  @Test
  public void testTableSourcePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    BigQueryIO.Read.Bound read = BigQueryIO.Read
        .from("project:dataset.tableId")
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("BigQueryIO.Read should include the table spec in its primitive display data",
        displayData, hasItem(hasDisplayItem("table")));
  }

  @Test
  public void testQuerySourcePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();
    BigQueryIO.Read.Bound read = BigQueryIO.Read
        .fromQuery("foobar")
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("BigQueryIO.Read should include the query in its primitive display data",
        displayData, hasItem(hasDisplayItem("query")));
  }

  @Test
  public void testBatchSinkPrimitiveDisplayData() {
    DataflowPipelineOptions options = DataflowDisplayDataEvaluator.getDefaultOptions();
    options.setStreaming(false);
    testSinkPrimitiveDisplayData(options);
  }

  @Test
  public void testStreamingSinkPrimitiveDisplayData() {
    DataflowPipelineOptions options = DataflowDisplayDataEvaluator.getDefaultOptions();
    options.setStreaming(true);
    testSinkPrimitiveDisplayData(options);
  }

  private void testSinkPrimitiveDisplayData(DataflowPipelineOptions options) {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create(options);

    BigQueryIO.Write.Bound write = BigQueryIO.Write
        .to("project:dataset.table")
        .withSchema(new TableSchema().set("col1", "type1").set("col2", "type2"))
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("BigQueryIO.Write should include the table spec in its primitive display data",
        displayData, hasItem(hasDisplayItem("tableSpec")));

    assertThat("BigQueryIO.Write should include the table schema in its primitive display data",
        displayData, hasItem(hasDisplayItem("schema")));
  }
}
