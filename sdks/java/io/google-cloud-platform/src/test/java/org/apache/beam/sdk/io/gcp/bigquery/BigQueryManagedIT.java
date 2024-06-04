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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryManagedIT {
  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("str", Schema.FieldType.STRING),
          Schema.Field.of("number", Schema.FieldType.INT64));

  private static final List<Row> ROWS =
      LongStream.range(0, 20)
          .mapToObj(
              i ->
                  Row.withSchema(SCHEMA)
                      .withFieldValue("str", Long.toString(i))
                      .withFieldValue("number", i)
                      .build())
          .collect(Collectors.toList());

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryManagedIT");

  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final String BIG_QUERY_DATASET_ID = "bigquery_managed_" + System.nanoTime();

  @BeforeClass
  public static void setUpTestEnvironment() throws IOException, InterruptedException {
    // Create one BQ dataset for all test cases.
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID, null);
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @Test
  public void testSimpleStorageWriteRead() {
    String table = String.format("%s:%s.managed_read_write", PROJECT, BIG_QUERY_DATASET_ID);

    Map<String, Object> writeConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", table)
            .put("create_disposition", "create_if_needed")
            .put("at_least_once", false)
            .build();
    Pipeline p = Pipeline.create();
    PCollectionRowTuple.of("input", p.apply(Create.of(ROWS)).setRowSchema(SCHEMA))
        .apply(Managed.write(Managed.BIGQUERY_STORAGE).withConfig(writeConfig));
    p.run().waitUntilFinish();

    Map<String, Object> readConfig =
        ImmutableMap.<String, Object>builder().put("table", table).build();
    Pipeline q = Pipeline.create();
    PCollection<Row> outputRows =
        PCollectionRowTuple.empty(p)
            .apply(Managed.read(Managed.BIGQUERY_STORAGE).withConfig(readConfig))
            .get(BigQueryDirectReadSchemaTransformProvider.OUTPUT_TAG);
    PAssert.that(outputRows).containsInAnyOrder(ROWS);
    q.run().waitUntilFinish();
  }
}
