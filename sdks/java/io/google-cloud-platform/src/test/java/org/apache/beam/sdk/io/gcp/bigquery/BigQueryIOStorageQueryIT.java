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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TableRowParser;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link BigQueryIO#read(SerializableFunction)} using {@link
 * Method#DIRECT_READ} to read query results. This test runs a simple "SELECT *" query over a
 * pre-defined table and asserts that the number of records read is equal to the expected count.
 */
@RunWith(JUnit4.class)
public class BigQueryIOStorageQueryIT {

  private static final Map<String, Long> EXPECTED_NUM_RECORDS =
      ImmutableMap.of(
          "empty", 0L,
          "1M", 10592L,
          "1G", 11110839L,
          "1T", 11110839000L);

  private static final String DATASET_ID = "big_query_storage";
  private static final String TABLE_PREFIX = "storage_read_";

  private BigQueryIOStorageQueryOptions options;

  /** Customized {@link TestPipelineOptions} for BigQueryIOStorageQuery pipelines. */
  public interface BigQueryIOStorageQueryOptions extends TestPipelineOptions, ExperimentalOptions {
    @Description("The table to be queried")
    @Validation.Required
    String getInputTable();

    void setInputTable(String table);

    @Description("The expected number of records")
    @Validation.Required
    long getNumRecords();

    void setNumRecords(long numRecords);
  }

  private void setUpTestEnvironment(String tableSize) {
    PipelineOptionsFactory.register(BigQueryIOStorageQueryOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigQueryIOStorageQueryOptions.class);
    options.setNumRecords(EXPECTED_NUM_RECORDS.get(tableSize));
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    options.setInputTable(project + '.' + DATASET_ID + '.' + TABLE_PREFIX + tableSize);
  }

  private void runBigQueryIOStorageQueryPipeline() {
    Pipeline p = Pipeline.create(options);
    PCollection<Long> count =
        p.apply(
                "Query",
                BigQueryIO.read(TableRowParser.INSTANCE)
                    .fromQuery("SELECT * FROM `" + options.getInputTable() + "`")
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ))
            .apply("Count", Count.globally());
    PAssert.thatSingleton(count).isEqualTo(options.getNumRecords());
    p.run().waitUntilFinish();
  }

  @Test
  public void testBigQueryStorageQuery1G() throws Exception {
    setUpTestEnvironment("1G");
    runBigQueryIOStorageQueryPipeline();
  }
}
