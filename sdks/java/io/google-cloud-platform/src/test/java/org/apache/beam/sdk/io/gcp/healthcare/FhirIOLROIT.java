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
  // The minimum allowed expiration time is one hour.
  private static Long BQ_TABLE_EXPIRATION_TIME_MS = 60*60*1000L;
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private BigqueryClient bqClient;
  private transient HealthcareApiClient client;
  private final String project;
  private String healthcareDataset;
  private final String fhirStoreId;
  private final String deidFhirStoreId;
  private final String bqDatasetId;
  private String version;

  public FhirIOLROIT() {
    this.bqClient = new BigqueryClient("FhirIOROIT");
    long testTime = System.currentTimeMillis();
    this.fhirStoreId = "FHIR_store_" + testTime + "_" + new SecureRandom().nextInt(32);
    this.deidFhirStoreId = fhirStoreId + "_deid";
    this.bqDatasetId = fhirStoreId + "_dataset";
    this.version = "STU3";
    this.project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
  }

  @Before
  public void setup() throws Exception {
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    client.createFhirStore(healthcareDataset, fhirStoreId, version, null);
    client.createFhirStore(healthcareDataset, deidFhirStoreId, version, null);
    bqClient.createNewDataset(project, bqDatasetId, BQ_TABLE_EXPIRATION_TIME_MS);

    // Execute bundles to populate some data.
    FhirIOTestUtil.executeFhirBundles(
        client,
        healthcareDataset + "/fhirStores/" + fhirStoreId,
        FhirIOTestUtil.BUNDLES.get(version));
  }

  @After
  public void deleteAllFhirStoresAndBqDataset() {
    try {
      HealthcareApiClient client = new HttpHealthcareApiClient();
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + fhirStoreId);
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + deidFhirStoreId);
      bqClient.deleteDataset(project, bqDatasetId);
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
    String fhirStoreName = healthcareDataset + "/fhirStores/" + fhirStoreId;
    String exportGcsUriPrefix =
        "gs://" + DEFAULT_TEMP_BUCKET + "/export/" + new SecureRandom().nextInt(32);
    pipeline.apply(FhirIO.exportResources(fhirStoreName, exportGcsUriPrefix));
    pipeline.run();
  }

  @Test
  public void test_FhirIO_exportFhirResources_BigQuery() {
    String fhirStoreName = healthcareDataset + "/fhirStores/" + fhirStoreId;
    String exportBqDatasetUri = String.format("bq://%s.%s", project, bqDatasetId);
    pipeline.apply(FhirIO.exportResources(fhirStoreName, exportBqDatasetUri));
    pipeline.run();
  }

  @Test
  public void test_FhirIO_deidentify() throws IOException {
    String fhirStoreName = healthcareDataset + "/fhirStores/" + fhirStoreId;
    String destinationFhirStoreName = healthcareDataset + "/fhirStores/" + deidFhirStoreId;
    DeidentifyConfig deidConfig = new DeidentifyConfig(); // use default DeidentifyConfig
    pipeline.apply(FhirIO.deidentify(fhirStoreName, destinationFhirStoreName, deidConfig));
    pipeline.run();
  }
}
