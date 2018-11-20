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
package org.apache.beam.examples.complete;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.beam.examples.complete.TrafficMaxLaneFlow.FormatMaxesFn;
import org.apache.beam.examples.complete.TrafficMaxLaneFlow.TrafficMaxLaneFlowOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of TrafficMaxLaneFlowIT. */
@RunWith(JUnit4.class)
public class TrafficMaxLaneFlowIT {
  private TrafficMaxLaneFlowOptions options;
  private final String timestamp = Long.toString(System.currentTimeMillis());
  private final String outputDatasetId = "traffic_max_lane_flow_" + timestamp;
  private final String outputTable = "traffic_max_lane_flow_table";
  private String projectId;
  private BigqueryClient bqClient;

  @Before
  public void setupTestEnvironment() {
    PipelineOptionsFactory.register(TrafficMaxLaneFlowOptions.class);
    this.options = TestPipeline.testingPipelineOptions().as(TrafficMaxLaneFlowOptions.class);
    this.projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    this.bqClient = new BigqueryClient("TrafficMaxLaneFlowIT");
    this.bqClient.createNewDataset(this.projectId, this.outputDatasetId);
  }

  @After
  public void cleanupTestEnvironment() {
    this.bqClient.deleteDataset(this.projectId, this.outputDatasetId);
  }

  @Test
  public void testE2ETrafficMaxLaneFlow() throws Exception {
    this.options.setBigQuerySchema(FormatMaxesFn.getSchema());
    this.options.setProject(this.projectId);
    this.options.setBigQueryDataset(this.outputDatasetId);
    this.options.setBigQueryTable(this.outputTable);
    TrafficMaxLaneFlow.runTrafficMaxLaneFlow(this.options);

    QueryResponse response =
        this.bqClient.queryWithRetries(
            String.format(
                "SELECT count(*) as total FROM [%s:%s.%s]",
                this.projectId, this.outputDatasetId, this.outputTable),
            this.projectId);
    String res = response.getRows().get(0).getF().get(0).getV().toString();
    assertEquals("9763", res);
  }
}
