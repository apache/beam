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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test that partitions and clusters sample data in BigQuery. */
@RunWith(JUnit4.class)
public class BigQueryTimePartitioningClusteringIT {
  private static final String WEATHER_SAMPLES_TABLE =
      "clouddataflow-readonly:samples.weather_stations";

  private Bigquery bqClient;
  private String datasetName = "BigQueryTimePartitioningIT";
  private BigQueryClusteringITOptions options;

  @Before
  public void setUp() {
    PipelineOptionsFactory.register(BigQueryClusteringITOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryClusteringITOptions.class);
    options.setTempLocation(options.getTempRoot() + "/temp-it/");
    bqClient = BigqueryClient.getNewBigquerryClient(options.getAppName());
  }

  /** Customized PipelineOptions for BigQueryClustering Integration Test. */
  public interface BigQueryClusteringITOptions
      extends TestPipelineOptions, ExperimentalOptions, BigQueryOptions {
    @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();

    void setInput(String value);
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

  @Test
  public void testE2EBigQueryTimePartitioning() throws Exception {
    String tableName = "weather_stations_time_partitioned_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", datasetName, tableName))
                .withTimePartitioning(getTimepartitioning())
                .withSchema(getSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    bqClient = BigqueryClient.getNewBigquerryClient(options.getAppName());
    Table table = bqClient.tables().get(options.getProject(), datasetName, tableName).execute();

    Assert.assertEquals(table.getTimePartitioning(), getTimepartitioning());
  }

  @Test
  public void testE2EBigQueryClustering() throws Exception {
    String tableName = "weather_stations_clustered_" + System.currentTimeMillis();

    Pipeline p = Pipeline.create(options);

    p.apply(BigQueryIO.readTableRows().from(options.getInput()))
        .apply(ParDo.of(new KeepStationNumberAndConvertDate()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(String.format("%s.%s", datasetName, tableName))
                .withTimePartitioning(getTimepartitioning())
                .withClustering(getClustering())
                .withSchema(getSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();

    Table table = bqClient.tables().get(options.getProject(), datasetName, tableName).execute();

    Assert.assertEquals(table.getClustering(), getClustering());
  }

  private TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("station_number").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("date").setType("DATE"));
    return new TableSchema().setFields(fields);
  }

  private TimePartitioning getTimepartitioning() {
    return new TimePartitioning().setField("date").setType("DAY");
  }

  private Clustering getClustering() {
    return new Clustering().setFields(Arrays.asList("station_number"));
  }
}
