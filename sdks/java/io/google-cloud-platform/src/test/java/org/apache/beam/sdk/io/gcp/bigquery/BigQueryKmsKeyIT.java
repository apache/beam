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
import static org.junit.Assert.assertNotNull;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.security.SecureRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.UsesKms;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for BigQuery operations that can use KMS keys, for use with DirectRunner.
 *
 * <p>Verification of KMS key usage is done on outputs, but not on any temporary files or tables
 * used.
 */
@RunWith(JUnit4.class)
@Category(UsesKms.class)
public class BigQueryKmsKeyIT {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryKmsKeyIT.class);

  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryKmsKeyIT");
  private static final String BIG_QUERY_DATASET_ID =
      "bq_query_to_table_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
  private static final TableSchema OUTPUT_SCHEMA =
      new TableSchema()
          .setFields(ImmutableList.of(new TableFieldSchema().setName("fruit").setType("STRING")));

  private static TestPipelineOptions options;
  private static String project;
  private static String kmsKey =
      "projects/apache-beam-testing/locations/global/keyRings/beam-it/cryptoKeys/test";

  @BeforeClass
  public static void setupTestEnvironment() throws Exception {
    options = TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    project = options.as(GcpOptions.class).getProject();
    BQ_CLIENT.createNewDataset(project, BIG_QUERY_DATASET_ID);
  }

  @AfterClass
  public static void cleanup() {
    LOG.info("Start to clean up tables and datasets.");
    BQ_CLIENT.deleteDataset(project, BIG_QUERY_DATASET_ID);
  }

  /**
   * Tests query job and table creation with KMS key settings.
   *
   * <p>Verifies table creation with KMS key.
   */
  private void testQueryAndWrite(Method method) throws Exception {
    String outputTableId = "testQueryAndWrite_" + method.name();
    String outputTableSpec = project + ":" + BIG_QUERY_DATASET_ID + "." + outputTableId;

    options.setTempLocation(
        FileSystems.matchNewDirectory(options.getTempRoot(), "bq_it_temp").toString());
    Pipeline p = Pipeline.create(options);
    // Reading triggers BQ query and extract jobs. Writing triggers either a load job or performs a
    // streaming insert (depending on method).
    p.apply(
            BigQueryIO.readTableRows()
                .fromQuery("SELECT * FROM (SELECT \"foo\" as fruit)")
                .withKmsKey(kmsKey))
        .apply(
            BigQueryIO.writeTableRows()
                .to(outputTableSpec)
                .withSchema(OUTPUT_SCHEMA)
                .withMethod(method)
                .withKmsKey(kmsKey));
    p.run().waitUntilFinish();

    Table table = BQ_CLIENT.getTableResource(project, BIG_QUERY_DATASET_ID, outputTableId);
    assertNotNull(String.format("table not found: %s", outputTableId), table);
    assertNotNull(
        "output table has no EncryptionConfiguration", table.getEncryptionConfiguration());
    assertEquals(table.getEncryptionConfiguration().getKmsKeyName(), kmsKey);
  }

  @Test
  public void testWithFileLoads() throws Exception {
    testQueryAndWrite(Method.FILE_LOADS);
  }

  @Test
  public void testWithStreamingInserts() throws Exception {
    testQueryAndWrite(Method.STREAMING_INSERTS);
  }
}
