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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.security.SecureRandom;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * An end-to-end test for {@link org.apache.beam.examples.cookbook.CombinePerKeyExamples}.
 *
 * This tests reads the public 'Shakespeare' data, and for each word in the dataset that is
 * over a given length, generates a string containing the list of play names in which that word
 * appears, and saves this information to a bigquery table.
 *
 * <p>Running instructions:
 *
 * <pre>
 *  ./gradlew integrationTest -p examples/java/ -DintegrationTestPipelineOptions='[
 *  "--tempLocation=gs://apache-beam-testing-developers/"]'
 *  --tests org.apache.beam.examples.cookbook.CombinePerKeyExamplesIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Check {@link org.apache.beam.examples.cookbook.CombinePerKeyExamples} form more configuration
 * options via PipelineOptions.
 */
@RunWith(JUnit4.class)
public class CombinePerKeyExamplesIT {
  private static final String PROJECT =
    TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  private static final BigQuery BIGQUERY =
    com.google.cloud.bigquery.BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
  private static final String DATASET =
    "CombinePerKeyExamplesIT" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
  private static final String DEFAULT_OUTPUT_CHECKSUM = "1ab4c7ec460b94bbb3c3885b178bf0e6bed56e1f";

  /** Options for the CombinePerKeyExamples Integration Test. */
  public interface CombinePerKeyExamplesITOptions
    extends TestPipelineOptions, CombinePerKeyExamples.Options, BigQueryOptions {}
  
  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @BeforeClass
  public static void beforeAll() throws Exception {
    BIGQUERY.create(DatasetInfo.newBuilder(PROJECT, DATASET).build());
  }

  @AfterClass
  public static void afterAll() {
    BIGQUERY.delete(DatasetId.of(PROJECT, DATASET), BigQuery.DatasetDeleteOption.deleteContents());
  }

  private void runE2ECombinePerKeyExamplesTest(CombinePerKeyExamplesIT.CombinePerKeyExamplesITOptions options) throws Exception {
    String query = String.format("SELECT word, all_plays FROM [%s]", options.getOutput());
    CombinePerKeyExamples.runCombinePerKeyExamples(options);

    assertThat(
      BigqueryMatcher.createQuery(options.getAppName(), options.getProject(), query),
      BigqueryMatcher.queryResultHasChecksum(DEFAULT_OUTPUT_CHECKSUM));
  }

  @Test
  public void testsE2ECombinePerKeyExamplesTest() throws Exception {
    CombinePerKeyExamplesIT.CombinePerKeyExamplesITOptions options =
      TestPipeline.testingPipelineOptions().as(CombinePerKeyExamplesIT.CombinePerKeyExamplesITOptions.class);
    options.setOutput(
      String.format(
        "%s.%s", DATASET, "words_in_plays_" + System.currentTimeMillis()));

    runE2ECombinePerKeyExamplesTest(options);
  }
}
