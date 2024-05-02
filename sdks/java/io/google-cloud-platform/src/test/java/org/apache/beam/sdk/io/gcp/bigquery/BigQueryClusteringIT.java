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
import java.math.BigInteger;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test that clusters sample data in BigQuery. */
@RunWith(JUnit4.class)
public class BigQueryClusteringIT {
  private static final Long EXPECTED_BYTES = 16000L;
  private static final BigInteger EXPECTED_ROWS = new BigInteger("1000");
  private static final String WEATHER_SAMPLES_TABLE =
      "apache-beam-testing.samples.weather_stations";
  private static final String DATASET_NAME = "BigQueryClusteringIT";
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

  @Before
  public void setUp() {
    PipelineOptionsFactory.register(BigQueryClusteringITOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryClusteringITOptions.class);
    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "temp-it").toString());
    bqClient = BigqueryClient.getNewBigqueryClient(options.getAppName());
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
      return new TableDestination(
          String.format("%s.%s", DATASET_NAME, tableName), null, null, CLUSTERING);
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
  public void testE2EBigQueryClusteringNoPartitionTableFunction() throws Exception {
    String tableName = "weather_stations_clustered_table_function_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(
                    (ValueInSingleWindow<TableRow> vsw) ->
                        new TableDestination(
                            String.format("%s.%s", DATASET_NAME, tableName),
                            null,
                            null,
                            CLUSTERING))
                .withClustering(CLUSTERING)
                .withSchema(SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withMethod(BigQueryIO.Write.Method.DEFAULT));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(CLUSTERING, table.getClustering());
    Assert.assertEquals(EXPECTED_BYTES, table.getNumBytes());
    Assert.assertEquals(EXPECTED_ROWS, table.getNumRows());
  }

  @Test
  public void testE2EBigQueryClusteringNoPartitionDynamicDestinations() throws Exception {
    String tableName =
        "weather_stations_clustered_dynamic_destinations_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getBqcInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(new ClusteredDestinations(tableName))
                .withJsonClustering(
                    ValueProvider.StaticValueProvider.of(
                        BigQueryHelpers.toJsonString(CLUSTERING.getFields())))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withMethod(BigQueryIO.Write.Method.FILE_LOADS));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), DATASET_NAME, tableName).execute();

    Assert.assertEquals(CLUSTERING, table.getClustering());
    Assert.assertEquals(EXPECTED_ROWS, table.getNumRows());
    Assert.assertEquals(EXPECTED_BYTES, table.getNumBytes());
  }
}
