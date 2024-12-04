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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** This class tests the execution of {@link Managed} BigQueryIO. */
@RunWith(JUnit4.class)
public class BigQueryManagedIT {
  @Rule public TestName testName = new TestName();
  @Rule public transient TestPipeline writePipeline = TestPipeline.create();
  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("str", Schema.FieldType.STRING),
          Schema.Field.of("number", Schema.FieldType.INT64),
          Schema.Field.of("dest", Schema.FieldType.INT64));

  private static final SerializableFunction<Long, Row> ROW_FUNC =
      l -> Row.withSchema(SCHEMA).addValue(Long.toString(l)).addValue(l).addValue(l % 3).build();

  private static final List<Row> ROWS =
      LongStream.range(0, 20).mapToObj(ROW_FUNC::apply).collect(Collectors.toList());

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
  public void testBatchFileLoadsWriteRead() {
    String table =
        String.format("%s.%s.%s", PROJECT, BIG_QUERY_DATASET_ID, testName.getMethodName());
    Map<String, Object> writeConfig = ImmutableMap.of("table", table);

    // file loads requires a GCS temp location
    String tempLocation = writePipeline.getOptions().as(TestPipelineOptions.class).getTempRoot();
    writePipeline.getOptions().setTempLocation(tempLocation);

    // batch write
    PCollectionRowTuple.of("input", getInput(writePipeline, false))
        .apply(Managed.write(Managed.BIGQUERY).withConfig(writeConfig));
    writePipeline.run().waitUntilFinish();

    Map<String, Object> readConfig =
        ImmutableMap.of("query", String.format("SELECT * FROM `%s`", table));
    // read and validate
    PCollection<Row> outputRows =
        readPipeline
            .apply(Managed.read(Managed.BIGQUERY).withConfig(readConfig))
            .getSinglePCollection();
    PAssert.that(outputRows).containsInAnyOrder(ROWS);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testStreamingStorageWriteRead() {
    String table =
        String.format("%s:%s.%s", PROJECT, BIG_QUERY_DATASET_ID, testName.getMethodName());
    Map<String, Object> config = ImmutableMap.of("table", table);

    if (writePipeline.getOptions().getRunner().getName().contains("DataflowRunner")) {
      // Need to manually enable streaming engine for legacy dataflow runner
      ExperimentalOptions.addExperiment(
          writePipeline.getOptions().as(ExperimentalOptions.class),
          GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    }

    // streaming write
    PCollectionRowTuple.of("input", getInput(writePipeline, true))
        .apply(Managed.write(Managed.BIGQUERY).withConfig(config));
    writePipeline.run().waitUntilFinish();

    // read and validate
    PCollection<Row> outputRows =
        readPipeline
            .apply(Managed.read(Managed.BIGQUERY).withConfig(config))
            .getSinglePCollection();
    PAssert.that(outputRows).containsInAnyOrder(ROWS);
    readPipeline.run().waitUntilFinish();
  }

  public void testDynamicDestinations(boolean streaming) throws IOException, InterruptedException {
    String baseTableName =
        String.format("%s:%s.dynamic_" + System.nanoTime(), PROJECT, BIG_QUERY_DATASET_ID);
    String destinationTemplate = baseTableName + "_{dest}";
    Map<String, Object> config =
        ImmutableMap.of("table", destinationTemplate, "drop", Collections.singletonList("dest"));

    if (!streaming) {
      // file loads requires a GCS temp location
      String tempLocation = writePipeline.getOptions().as(TestPipelineOptions.class).getTempRoot();
      writePipeline.getOptions().setTempLocation(tempLocation);
    }

    // write
    PCollectionRowTuple.of("input", getInput(writePipeline, streaming))
        .apply(Managed.write(Managed.BIGQUERY).withConfig(config));
    writePipeline.run().waitUntilFinish();

    List<String> destinations =
        Arrays.asList(baseTableName + "_0", baseTableName + "_1", baseTableName + "_2");

    // read and validate each table destination
    RowFilter rowFilter = new RowFilter(SCHEMA).drop(Collections.singletonList("dest"));
    for (int i = 0; i < destinations.size(); i++) {
      long mod = i;
      String dest = destinations.get(i);
      List<Row> writtenRows =
          BQ_CLIENT
              .queryUnflattened(String.format("SELECT * FROM [%s]", dest), PROJECT, true, false)
              .stream()
              .map(tableRow -> BigQueryUtils.toBeamRow(rowFilter.outputSchema(), tableRow))
              .collect(Collectors.toList());

      List<Row> expectedRecords =
          ROWS.stream()
              .filter(row -> row.getInt64("dest") == mod)
              .map(rowFilter::filter)
              .collect(Collectors.toList());

      assertThat(writtenRows, containsInAnyOrder(expectedRecords.toArray()));
    }
  }

  @Test
  public void testStreamingDynamicDestinations() throws IOException, InterruptedException {
    if (writePipeline.getOptions().getRunner().getName().contains("DataflowRunner")) {
      // Need to manually enable streaming engine for legacy dataflow runner
      ExperimentalOptions.addExperiment(
          writePipeline.getOptions().as(ExperimentalOptions.class),
          GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    }
    testDynamicDestinations(true);
  }

  @Test
  public void testBatchDynamicDestinations() throws IOException, InterruptedException {
    testDynamicDestinations(false);
  }

  public PCollection<Row> getInput(Pipeline p, boolean isStreaming) {
    if (isStreaming) {
      return p.apply(
              PeriodicImpulse.create()
                  .startAt(new Instant(0))
                  .stopAt(new Instant(19))
                  .withInterval(Duration.millis(1)))
          .apply(MapElements.into(TypeDescriptors.rows()).via(i -> ROW_FUNC.apply(i.getMillis())))
          .setRowSchema(SCHEMA);
    }
    return p.apply(Create.of(ROWS)).setRowSchema(SCHEMA);
  }
}
