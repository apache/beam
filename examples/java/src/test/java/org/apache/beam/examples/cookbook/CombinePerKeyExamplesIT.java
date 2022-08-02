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

import static org.junit.Assert.assertEquals;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** An end-to-end test for {@link org.apache.beam.examples.cookbook.CombinePerKeyExamples}. */
@RunWith(JUnit4.class)
public class CombinePerKeyExamplesIT {
  private CombinePerKeyExamplesOptions options;
  private final String timestamp = Long.toString(System.currentTimeMillis());
  private final String outputDatasetId = "combine_per_key_examples" + timestamp;
  private final String outputTable = "combine_per_key_examples_table";
  private final Long defaultExpiration = 1000L * 60 * 60;
  private String projectId;
  private BigqueryClient bqClient;

  /** Options for the CombinePerKeyExamples Integration Test. */
  public interface CombinePerKeyExamplesOptions
      extends TestPipelineOptions, CombinePerKeyExamples.Options {}

  @Before
  public void setupTestEnvironment() throws Exception {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    this.options = TestPipeline.testingPipelineOptions().as(CombinePerKeyExamplesOptions.class);
    this.projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    this.bqClient = new BigqueryClient("CombinePerKeyExamplesIT");
    this.bqClient.createNewDataset(this.projectId, this.outputDatasetId, defaultExpiration);
  }

  @After
  public void cleanupTestEnvironment() {
    this.bqClient.deleteDataset(this.projectId, this.outputDatasetId);
  }

  @Test
  public void testE2ECombinePerKey() throws Exception {
    this.options.setOutput(
        String.format("%s:%s.%s", this.projectId, this.outputDatasetId, this.outputTable));
    CombinePerKeyExamples.runCombinePerKeyExamples(options);
    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT.withMaxRetries(4).withInitialBackoff(Duration.standardSeconds(1L));
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff = BackOffAdapter.toGcpBackOff(backoffFactory.backoff());
    String res = "empty_result";
    do {
      QueryResponse response =
          this.bqClient.queryWithRetries(
              String.format(
                  "SELECT count(*) as total FROM [%s:%s.%s]",
                  this.projectId, this.outputDatasetId, this.outputTable),
              this.projectId);
      // Having 4 retries to get ride of the failure caused by the latency that between data wrote
      // to BigQuery and be able to query from BigQuery.
      // Partial results are still returned making traversal of nested result object NPE prone.
      try {
        res = response.getRows().get(0).getF().get(0).getV().toString();
        break;
      } catch (NullPointerException e) {
        // Ignore NullPointerException during retry.
      }
    } while (BackOffUtils.next(sleeper, backoff));

    assertEquals("8045", res);
  }
}
