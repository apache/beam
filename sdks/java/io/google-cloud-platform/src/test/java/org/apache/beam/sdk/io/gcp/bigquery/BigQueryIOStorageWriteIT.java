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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link BigQueryIO#write()}. The batch mode tests write 30MB data to BQ and
 * verify the written row count; the streaming mode tests write 3k rows of data to BQ and verify the
 * written row count.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageWriteIT {

  private enum WriteMode {
    EXACT_ONCE,
    AT_LEAST_ONCE
  }

  private String project;
  private static final String DATASET_ID = "big_query_storage";
  private static final String TABLE_PREFIX = "storage_write_";

  private BigQueryOptions bqOptions;
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryStorageIOWriteIT");

  private void setUpTestEnvironment(WriteMode writeMode) {
    PipelineOptionsFactory.register(BigQueryOptions.class);
    bqOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
    bqOptions.setProject(TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject());
    bqOptions.setUseStorageWriteApi(true);
    if (writeMode == WriteMode.AT_LEAST_ONCE) {
      bqOptions.setUseStorageWriteApiAtLeastOnce(true);
    }
    bqOptions.setNumStorageWriteApiStreams(2);
    bqOptions.setStorageWriteApiTriggeringFrequencySec(1);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  }

  static class FillRowFn extends DoFn<Long, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new TableRow().set("number", c.element()).set("str", "aaaaaaaaaa"));
    }
  }

  static class UnboundedStream extends PTransform<PBegin, PCollection<Long>> {

    private final int rowCount;

    public UnboundedStream(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public PCollection<Long> expand(PBegin input) {
      int timestampIntervalInMillis = 10;
      PeriodicImpulse impulse =
          PeriodicImpulse.create()
              .stopAfter(Duration.millis((long) timestampIntervalInMillis * rowCount - 1))
              .withInterval(Duration.millis(timestampIntervalInMillis));
      return input
          .apply(impulse)
          .apply(
              MapElements.via(
                  new SimpleFunction<Instant, Long>() {
                    @Override
                    public Long apply(Instant input) {
                      return input.getMillis();
                    }
                  }));
    }
  }

  private void runBigQueryIOStorageWritePipeline(
      int rowCount, WriteMode writeMode, Boolean isStreaming) {
    String tableName =
        isStreaming
            ? TABLE_PREFIX + "streaming_" + System.currentTimeMillis()
            : TABLE_PREFIX + System.currentTimeMillis();
    TableSchema schema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("str").setType("STRING")));

    Pipeline p = Pipeline.create(bqOptions);
    p.apply(
            "Input",
            isStreaming ? new UnboundedStream(rowCount) : GenerateSequence.from(0).to(rowCount))
        .apply("GenerateMessage", ParDo.of(new FillRowFn()))
        .apply(
            "WriteToBQ",
            BigQueryIO.writeTableRows()
                .to(String.format("%s:%s.%s", project, DATASET_ID, tableName))
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    p.run().waitUntilFinish();
    String testQuery = String.format("SELECT count(*) FROM [%s.%s];", DATASET_ID, tableName);
    try {
      QueryResponse response = BQ_CLIENT.queryWithRetries(testQuery, project);
      if (writeMode == WriteMode.EXACT_ONCE) {
        assertEquals(
            Integer.parseInt((String) response.getRows().get(0).getF().get(0).getV()), rowCount);
      } else {
        assertTrue(
            Integer.parseInt((String) response.getRows().get(0).getF().get(0).getV()) >= rowCount);
      }
    } catch (IOException | InterruptedException e) {
      fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void testBigQueryStorageWrite3MProto() {
    setUpTestEnvironment(WriteMode.EXACT_ONCE);
    runBigQueryIOStorageWritePipeline(3_000_000, WriteMode.EXACT_ONCE, false);
  }

  @Test
  public void testBigQueryStorageWrite3MProtoALO() {
    setUpTestEnvironment(WriteMode.AT_LEAST_ONCE);
    runBigQueryIOStorageWritePipeline(3_000_000, WriteMode.AT_LEAST_ONCE, false);
  }

  @Test
  public void testBigQueryStorageWrite3KProtoStreaming() {
    setUpTestEnvironment(WriteMode.EXACT_ONCE);
    runBigQueryIOStorageWritePipeline(3000, WriteMode.EXACT_ONCE, true);
  }

  @Test
  public void testBigQueryStorageWrite3KProtoALOStreaming() {
    setUpTestEnvironment(WriteMode.AT_LEAST_ONCE);
    runBigQueryIOStorageWritePipeline(3000, WriteMode.AT_LEAST_ONCE, true);
  }
}
