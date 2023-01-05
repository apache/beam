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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Integration test for reading and writing JSON data to/from BigQuery */
@RunWith(JUnit4.class)
public class BigQueryIOJsonIT {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIOJsonIT.class);

  private static PipelineOptions testOptions = TestPipeline.testingPipelineOptions();

  static {
    TestPipelineOptions opt = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    testOptions.setTempLocation(opt.getTempRoot() + "/java-tmp");
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(testOptions);

  @Test
  public void testRepro() {
    String testQuery = "select cast(\"2022-12-21\" as date) dt";

    PCollection<TableRow> tableRows = pipeline.apply(BigQueryIO.readTableRowsWithSchema()
        .fromQuery(testQuery)
        .usingStandardSql()
    );
    PCollection<Row> rows = tableRows.apply(MapElements.into(TypeDescriptors.rows())
            .via(tableRows.getToRowFunction()))
        .setRowSchema(tableRows.getSchema());
    rows.apply("println", ParDo.of(new DoFn<Row, Row>() {
      @ProcessElement
      public void processElement(@Element Row row) {
        System.out.println("println row" + row);
        LOG.warn("row: " + row);
      }
    })).setRowSchema(tableRows.getSchema());
    pipeline.run();
  }
}
