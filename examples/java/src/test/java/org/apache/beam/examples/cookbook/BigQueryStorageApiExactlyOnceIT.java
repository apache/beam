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
package org.apache.beam.examples.cookbook;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryStorageApiExactlyOnceIT {

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("StorageApiSinkFailedRowsIT");
  private static final String PROJECT =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

  private static final String BIG_QUERY_DATASET_ID =
      "storage_api_sink_exactly_once" + System.nanoTime();

  @BeforeClass
  public static void setUp() throws IOException, InterruptedException {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    BQ_CLIENT.createNewDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    BQ_CLIENT.deleteDataset(PROJECT, BIG_QUERY_DATASET_ID);
  }

  String getTableName(
      BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options,
      boolean isGolden) {
    return String.format("`%s`",
            isGolden ? options.getDestinationTable() : options.getDestinationTable() + "_golden");
  }

  String getFrom(
          BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options,
          boolean isGolden,
          boolean removeDuplicates) {
    String tableName = getTableName(options, isGolden);
    if (removeDuplicates) {
      return String.format("(SELECT k.number FROM (SELECT ARRAY_AGG(x LIMIT 1)[OFFSET(0)] k FROM %s x GROUP BY number))",
              tableName);
    } else {
      return tableName;
    }
  }

  String getCountQuery(
      BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options,
      boolean isGolden,
      boolean removeDuplicates) {
    return String.format("SELECT COUNT(*) FROM %s", getFrom(options, isGolden, removeDuplicates));
  }

  String getSumQuery(
      BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options,
      boolean isGolden,
      boolean removeDuplicates) {
    return String.format("SELECT SUM(number) FROM %s", getFrom(options, isGolden, removeDuplicates));
  }

  long runQueryAndGetResult(String query) throws Exception {
    TableRow queryResponse =
        Iterables.getOnlyElement(BQ_CLIENT.queryUnflattened(query, PROJECT, true, true));
    return Long.parseLong((String) queryResponse.get("f0_"));
  }

  void runTest(boolean streaming,
               BigQueryIO.Write.Method method,
               BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options) {
    options.setDestinationTable(
            PROJECT + "." + BIG_QUERY_DATASET_ID + "." + "table" + System.nanoTime());
    if (streaming) {
      options
              .as(BigQueryOptions.class)
              .setCrashStorageApiWriteEverySeconds(options.getParDoCrashDuration());
      options.as(StreamingOptions.class).setStreaming(true);
    }
    Pipeline p = Pipeline.create(options);
    p.apply(new BigQueryStorageApiExactlyOnceTransform(method));
    p.run().waitUntilFinish();

  }
  @Test
  public void testExactlyOnceStreaming() throws Exception {
    BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options =
            TestPipeline.testingPipelineOptions()
                    .as(BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions.class);
    runTest(true, BigQueryIO.Write.Method.STORAGE_WRITE_API, options);
    assertThat(
        runQueryAndGetResult(getCountQuery(options, false, false)),
        Matchers.equalTo(runQueryAndGetResult(getCountQuery(options, true, false))));
    assertThat(
        runQueryAndGetResult(getSumQuery(options, false, false)),
        Matchers.equalTo(runQueryAndGetResult(getSumQuery(options, true, false))));
  }

  @Test
  public void testExactlyOnceBatch() throws Exception {
    BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options =
            TestPipeline.testingPipelineOptions()
                    .as(BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions.class);
    if (options.getNumElements() <= 0) {
      options.setNumElements(50000000L);
    }
    runTest(false, BigQueryIO.Write.Method.STORAGE_WRITE_API, options);
    assertThat(
            runQueryAndGetResult(getCountQuery(options, false, false)),
            Matchers.equalTo(runQueryAndGetResult(getCountQuery(options, true, false))));
    assertThat(
            runQueryAndGetResult(getSumQuery(options, false, false)),
            Matchers.equalTo(runQueryAndGetResult(getSumQuery(options, true, false))));
  }

  @Test
  public void testAtLeastOnce() throws Exception {
    BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions options =
            TestPipeline.testingPipelineOptions()
                    .as(BigQueryStorageApiExactlyOnceTransform.BigQueryStorageApiExactlyOnceOptions.class);
    options.as(BigQueryOptions.class).setUseStorageApiConnectionPool(true);

    runTest(true, BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE, options);
    assertThat(
            runQueryAndGetResult(getCountQuery(options, false, true)),
            Matchers.equalTo(runQueryAndGetResult(getCountQuery(options, true, false))));
    assertThat(
            runQueryAndGetResult(getSumQuery(options, false, true)),
            Matchers.equalTo(runQueryAndGetResult(getSumQuery(options, true, false))));
  }
  }
