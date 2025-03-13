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
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.DEFAULT_TEMP_BUCKET;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.HEALTHCARE_DATASET_TEMPLATE;

import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import java.io.IOException;
import java.security.SecureRandom;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOLROIT {
  private static final String FHIR_VERSION = "STU3";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private final transient HealthcareApiClient client;
  private final String project;
  private final String healthcareDataset;
  private final String fhirStoreId;
  private final String fhirStoreName;
  private final String deidFhirStoreId;
  private final String deidFhirStoreName;

  public FhirIOLROIT() throws IOException {
    this.client = new HttpHealthcareApiClient();
    this.fhirStoreId =
        "FHIR_store_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
    this.deidFhirStoreId = fhirStoreId + "_deid";
    this.project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    this.healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    final String fhirStorePrefix = healthcareDataset + "/fhirStores/";
    this.fhirStoreName = fhirStorePrefix + fhirStoreId;
    this.deidFhirStoreName = fhirStorePrefix + deidFhirStoreId;
  }

  @Before
  public void setup() throws Exception {
    client.createFhirStore(healthcareDataset, fhirStoreId, FHIR_VERSION, null);
    client.createFhirStore(healthcareDataset, deidFhirStoreId, FHIR_VERSION, null);

    // Execute bundles to populate some data.
    FhirIOTestUtil.executeFhirBundles(
        client, fhirStoreName, FhirIOTestUtil.BUNDLES.get(FHIR_VERSION));
  }

  @After
  public void deleteAllFhirStores() {
    try {
      client.deleteFhirStore(fhirStoreName);
      client.deleteFhirStore(deidFhirStoreName);
    } catch (IOException e) {
      // Do nothing.
    }
  }

  @AfterClass
  public static void teardownBucket() throws IOException {
    FhirIOTestUtil.tearDownTempBucket();
  }

  @Test
  public void test_FhirIO_exportFhirResources_Gcs() {
    final String exportGcsUriPrefix =
        "gs://" + DEFAULT_TEMP_BUCKET + "/export/" + new SecureRandom().nextInt(32);
    pipeline.apply(FhirIO.exportResources(fhirStoreName, exportGcsUriPrefix));
    pipeline.run();
  }

  @Test
  public void test_FhirIO_exportFhirResources_BigQuery() throws IOException, InterruptedException {
    final String bqDatasetId = fhirStoreId + "_dataset";
    final BigqueryClient bqClient = BigqueryClient.getClient("FhirIOROIT");
    bqClient.createNewDataset(
        project, bqDatasetId, 60 * 60 * 1000L); // min allowed expiration time is 1 hour.
    final String exportBqDatasetUri = String.format("bq://%s.%s", project, bqDatasetId);
    pipeline.apply(FhirIO.exportResources(fhirStoreName, exportBqDatasetUri));
    pipeline.run();
    bqClient.deleteDataset(project, bqDatasetId);
  }

  @Test
  public void test_FhirIO_deidentify() throws IOException {
    DeidentifyConfig deidConfig = new DeidentifyConfig(); // use default DeidentifyConfig
    pipeline.apply(FhirIO.deidentify(fhirStoreName, deidFhirStoreName, deidConfig));
    pipeline.run();
  }
}
