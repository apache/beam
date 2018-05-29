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

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests of BigQueryTornadoes.
 */
@RunWith(JUnit4.class)
public class BigQueryTornadoesIT {

  private static final String DEFAULT_OUTPUT_CHECKSUM = "1ab4c7ec460b94bbb3c3885b178bf0e6bed56e1f";

  /**
   * Options for the BigQueryTornadoes Integration Test.
   */
  public interface BigQueryTornadoesITOptions
      extends TestPipelineOptions, BigQueryTornadoes.Options, BigQueryOptions {
  }

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EBigQueryTornadoes() throws Exception {
    BigQueryTornadoesITOptions options =
        TestPipeline.testingPipelineOptions().as(BigQueryTornadoesITOptions.class);
    options.setOutput(String.format("%s.%s",
        "BigQueryTornadoesIT", "monthly_tornadoes_" + System.currentTimeMillis()));

    String query =
        String.format("SELECT month, tornado_count FROM [%s]", options.getOutput());
    options.setOnSuccessMatcher(
        new BigqueryMatcher(
            options.getAppName(), options.getProject(), query, DEFAULT_OUTPUT_CHECKSUM));

    BigQueryTornadoes.runBigQueryTornadoes(options);
  }
}
