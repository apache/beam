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

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link
 * org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO#write(SerializableFunction)}. This test writes
 * 30MB data to BQ and verify the written row count.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageWriteIT {

  private enum WriteMode {
    EXACT_ONCE,
    AT_LEAST_ONCE
  };

  private String project;
  private static final String DATASET_ID = "big_query_storage";
  private static final String TABLE_PREFIX = "storage_write_";

  private BigQueryOptions bqOptions;
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryStorageIOWriteIT");

  private void setUpTestEnvironment(WriteMode writeMode) {
    PipelineOptionsFactory.register(BigQueryOptions.class);
    bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject(TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject());
    if (writeMode == WriteMode.EXACT_ONCE) {
      bqOptions.setUseStorageWriteApi(true);
    } else {
      bqOptions.setUseStorageWriteApiAtLeastOnce(true);
    }
    bqOptions.setNumStorageWriteApiStreams(2);
    bqOptions.setStorageWriteApiTriggeringFrequencySec(1);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  }

  private void runBigQueryIOStorageWritePipeline(int rowCount) {
    String tableName = TABLE_PREFIX + System.currentTimeMillis();
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("str").setType("STRING")));

    Pipeline p = Pipeline.create(bqOptions);
    // Assumes has size 10 bytes of data.
    TableRow rowToInsert = new TableRow().set("number", 0).set("str", "aaaaaaaaaa");
    Create.Values<TableRow> input = Create.<TableRow>of(rowToInsert);

    p.apply("Input", input)
        .apply(
            "Generate30M",
            ParDo.of(
                new DoFn<TableRow, ArrayList<TableRow>>() {
                  public void processElement(ProcessContext c) {
                    TableRow row = c.element();
                    ArrayList<TableRow> rows = new ArrayList<TableRow>(300000);
                    for (int i = 0; i < rowCount; i++) {
                      rows.add(row);
                    }
                    c.ouput(rows);
                  }
                }))
        .apply(
            "WriteToBQ",
            BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.%s", project, DATASET_ID, tableName))
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run().waitUntilFinish();
    String testQuery = String.format("SELECT count(*) FROM [%s.%s];", DATASET_ID, tableName);
    QueryResponse response = BQ_CLIENT.queryWithRetries(testQuery, project);
    assertEquals(rowCount, response.getRows().at(0).getF().at(0).getV());
  }

  @Test
  public void testBigQueryStorageWrite30MProto() throws Exception {
    setUpTestEnvironment(WriteMode.EXACT_ONCE);
    runBigQueryIOStorageWritePipeline(3000000);
  }

  @Test
  public void testBigQueryStorageWrite30MProtoALO() throws Exception {
    setUpTestEnvironment(WriteMode.AT_LEAST_ONCE);
    runBigQueryIOStorageWritePipeline(3000000);
  }
}
