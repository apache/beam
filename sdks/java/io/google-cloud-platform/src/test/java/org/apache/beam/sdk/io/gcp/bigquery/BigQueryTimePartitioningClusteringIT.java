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

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import java.security.SecureRandom;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test that partitions and clusters sample data in BigQuery. */
@RunWith(JUnit4.class)
public class BigQueryTimePartitioningClusteringIT {
  private static final String WEATHER_SAMPLES_TABLE =
      "apache-beam-testing.samples.weather_stations";

  private static String project;
  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("BigQueryTimePartitioningClusteringIT");
  private static final String DATASET_NAME =
      "BigQueryTimePartitioningIT_"
          + System.currentTimeMillis()
          + "_"
          + new SecureRandom().nextInt(32);
  private static final TimePartitioning TIME_PARTITIONING =
      new TimePartitioning().setField("date").setType("DAY");
  private static final Clustering CLUSTERING =
      new Clustering().setFields(Arrays.asList("station_number"));
  private static final TableSchema SCHEMA =
      new TableSchema()
          .setFields(
              Arrays.asList(
                  new TableFieldSchema().setName("station_number").setType("INTEGER"),
                  new TableFieldSchema().setName("date").setType("DATE")));

  private Bigquery bqClient;
  private BigQueryClusteringITOptions options;

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    BQ_CLIENT.createNewDataset(
        project,
        DATASET_NAME,
        null,
        TestPipeline.testingPipelineOptions().as(TestBigQueryOptions.class).getBigQueryLocation());
  }

  @Before
  public void setUp() {
    PipelineOptionsFactory.register(BigQueryClusteringITOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryClusteringITOptions.class);
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "temp-it").toString());
    bqClient = BigqueryClient.getNewBigqueryClient(options.getAppName());
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET_NAME);
  }

  /** Customized PipelineOptions for BigQueryClustering Integration Test. */
  public interface BigQueryClusteringITOptions
      extends TestPipelineOptions, ExperimentalOptions, BigQueryOptions {
    @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getBqcInput();

    void setBqcInput(String value);
  }

  static class KeepStationNumberAndConvertDate extends DoFn<TableRow, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String day = (String) c.element().get("day");
      String month = (String) c.element().get("month");
      String year = (String) c.element().get("year");

      TableRow row = new TableRow();
      row.set("station_number", c.element().get("station_number"));
      row.set("date", String.format("%s-%s-%s", year, month, day));
      c.output(row);
    }
  }

  static class ClusteredDestinations extends DynamicDestinations<TableRow, TableDestination> {
    private final String tableName;

    public ClusteredDestinations(String tableName) {
      this.tableName = tableName;
    }

    @Override
    public @Nullable Coder<TableDestination> getDestinationCoder() {
      return TableDestinationCoderV3.of();
    }

    @Override
    public TableDestination getDestination(ValueInSingleWindow<TableRow> element) {
      return new TableDestination(tableName, null, TIME_PARTITIONING, CLUSTERING);
    }

    @Override
    public TableDestination getTable(TableDestination destination) {
      return destination;
    }

    @Override
    public TableSchema getSchema(TableDestination destination) {
      return SCHEMA;
    }
  }

  @Test
  public void testE2EBigQueryTimePartitioning() throws Exception {
    String tableName = "weather_stations_time_partitioned_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", DATASET_NAME, tableName))
                .withTimePartitioning(TIME_PARTITIONING)
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    bqClient = BigqueryClient.getNewBigqueryClient(options.getAppName());
    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(table.getTimePartitioning(), TIME_PARTITIONING);
  }

  @Test
  public void testE2EBigQueryClustering() throws Exception {
    String tableName = "weather_stations_clustered_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", DATASET_NAME, tableName))
                .withTimePartitioning(TIME_PARTITIONING)
                .withClustering(CLUSTERING)
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(table.getClustering(), CLUSTERING);
  }

  @Test
  public void testE2EBigQueryClusteringTableFunction() throws Exception {
    String tableName = "weather_stations_clustered_table_function_" + System.currentTimeMillis();
    String destination = String.format("%s.%s", DATASET_NAME, tableName);

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(
                    (ValueInSingleWindow<TableRow> vsw) ->
                        new TableDestination(destination, null, TIME_PARTITIONING, CLUSTERING))
                .withClustering()
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(table.getClustering(), CLUSTERING);
    Assert.assertEquals(table.getTimePartitioning(), TIME_PARTITIONING);
  }

  @Test
  public void testE2EBigQueryClusteringDynamicDestinations() throws Exception {
    String tableName =
        "weather_stations_clustered_dynamic_destinations_" + System.currentTimeMillis();
    String destination = String.format("%s.%s", DATASET_NAME, tableName);

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(new ClusteredDestinations(destination))
                .withClustering()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(table.getClustering(), CLUSTERING);
  }
}
