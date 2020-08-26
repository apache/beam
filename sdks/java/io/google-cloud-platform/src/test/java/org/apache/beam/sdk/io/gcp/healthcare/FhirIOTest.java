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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private final String fhirStoreId;
  private final String project;
  private transient HealthcareApiClient client;
  private String healthcareDataset;
  private String version;

  public FhirIOTest() {
    long testTime = System.currentTimeMillis();
    this.fhirStoreId = "FHIR_store_" + testTime + "_" + (new SecureRandom().nextInt(32));
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

    // Execute bundles to populate some data.
    FhirIOTestUtil.executeFhirBundles(
        client,
        healthcareDataset + "/fhirStores/" + fhirStoreId,
        FhirIOTestUtil.BUNDLES.get(version));
  }

  @After
  public void deleteFHIRtore() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteFhirStore(healthcareDataset + "/fhirStores/" + fhirStoreId);
  }

  @AfterClass
  public static void teardownBucket() throws IOException {
    FhirIOTestUtil.tearDownTempBucket();
  }

  @Test
  public void test_FhirIO_exportFhirResourcesGcs() {
    String fhirStoreName = healthcareDataset + "/fhirStores/" + fhirStoreId;
    String exportGcsUriPrefix = "gs://" + DEFAULT_TEMP_BUCKET + "/"
        + (new SecureRandom().nextInt(32));
    FhirIO.ExportGcs.Result exportResults =
        pipeline.apply(FhirIO.exportResourcesToGcs(fhirStoreName, exportGcsUriPrefix));
    PCollection<String> resources = exportResults.getResources();
    resources.apply(TextIO.write().to(exportGcsUriPrefix + "/write"));
    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedReads() {
    List<String> badMessageIDs = Arrays.asList("foo", "bar");
    FhirIO.Read.Result readResult =
        pipeline.apply(Create.of(badMessageIDs)).apply(FhirIO.readResources());

    PCollection<HealthcareIOError<String>> failed = readResult.getFailedReads();

    PCollection<String> resources = readResult.getResources();

    PCollection<String> failedMsgIds =
        failed.apply(
            MapElements.into(TypeDescriptors.strings()).via(HealthcareIOError::getDataResource));

    PAssert.that(failedMsgIds).containsInAnyOrder(badMessageIDs);
    PAssert.that(resources).empty();
    pipeline.run();
  }

  @Test
  public void test_FhirIO_failedWrites() {
    String badBundle = "bad";
    List<String> emptyMessages = Collections.singletonList(badBundle);

    PCollection<String> fhirBundles = pipeline.apply(Create.of(emptyMessages));

    FhirIO.Write.Result writeResult =
        fhirBundles.apply(
            FhirIO.Write.executeBundles(
                "projects/foo/locations/us-central1/datasets/bar/hl7V2Stores/baz"));

    PCollection<HealthcareIOError<String>> failedInserts = writeResult.getFailedBodies();

    PAssert.thatSingleton(failedInserts)
        .satisfies(
            (HealthcareIOError<String> err) -> {
              Assert.assertEquals("bad", err.getDataResource());
              return null;
            });
    PCollection<Long> numFailedInserts = failedInserts.apply(Count.globally());

    PAssert.thatSingleton(numFailedInserts).isEqualTo(1L);

    pipeline.run();
  }

  private static final long NUM_ELEMENTS = 11;

  private static ArrayList<KV<String, String>> createTestData() {
    String[] scientists = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
    };
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < NUM_ELEMENTS; i++) {
      int index = i % scientists.length;
      KV<String, String> element = KV.of("key", scientists[index]);
      data.add(element);
    }
    return data;
  }
}
