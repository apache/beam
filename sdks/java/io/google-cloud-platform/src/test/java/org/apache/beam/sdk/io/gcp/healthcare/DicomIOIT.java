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

import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.HEALTHCARE_DATASET_TEMPLATE;

import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.DicomConfig;
import com.google.api.services.healthcare.v1.model.TagFilterList;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToJson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class DicomIOIT {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private String healthcareDataset;
  private String project;
  private HealthcareApiClient client;
  private final String dicomStoreId;
  private final String dicomStoreName;
  private final String deidDicomStoreId;
  private final String deidDicomStoreName;
  private static final String TEST_FILE_STUDY_ID = "study_000000000";

  public DicomIOIT() throws IOException {
    this.client = new HttpHealthcareApiClient();
    this.dicomStoreId =
        "DICOM_store_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt(32);
    this.deidDicomStoreId = dicomStoreId + "_deid";
    this.project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    this.healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    final String dicomStorePrefix = healthcareDataset + "/dicomStores/";
    this.dicomStoreName = dicomStorePrefix + dicomStoreId;
    this.deidDicomStoreName = dicomStorePrefix + deidDicomStoreId;
  }

  @Before
  public void setup() throws IOException, URISyntaxException {
    try {
      client.createDicomStore(healthcareDataset, dicomStoreName);
      client.createDicomStore(healthcareDataset, deidDicomStoreName);
    } catch (IOException e) {
      // Do nothing.
    }
  }

  @After
  public void deleteAllDicomStores() {
    try {
      client.deleteDicomStore(dicomStoreName);
      client.deleteFhirStore(deidDicomStoreName);
    } catch (IOException e) {
      // Do nothing.
    }
  }

  @Ignore("https://github.com/apache/beam/issues/28099")
  @Test
  public void testDicomMetadataRead() throws IOException {
    String webPath =
        String.format(
            "%s/dicomStores/%s/dicomWeb/studies/%s",
            healthcareDataset, dicomStoreName, TEST_FILE_STUDY_ID);

    DicomIO.ReadStudyMetadata.Result result =
        pipeline.apply(Create.of(webPath)).apply(DicomIO.readStudyMetadata());

    PAssert.that(result.getFailedReads()).empty();
    PAssert.that(result.getReadResponse())
        .satisfies(
            input -> {
              for (String resp : input) {
                Assert.assertTrue(resp.contains(TEST_FILE_STUDY_ID));
              }
              return null;
            });

    PipelineResult job = pipeline.run();

    try {
      job.cancel();
    } catch (UnsupportedOperationException exc) {
      // noop - if runner does not support job.cancel()
    }
  }

  @Test
  public void test_DicomIO_deidentify() {
    DicomConfig dicomConfig = new DicomConfig().setFilterProfile("ATTRIBUTE_CONFIDENTIALITY_BASIC_PROFILE");
    DeidentifyConfig deidConfig = new DeidentifyConfig().set("",dicomConfig); // use default DeidentifyConfig
    DicomIO.Deidentify.Result result =
        pipeline.apply(DicomIO.deidentify("projects/apache-beam-testing/locations/us-central1/datasets/apache-beam-integration-testing/dicomStores/dicom_it_persistent_store", "projects/apache-beam-testing/locations/us-central1/datasets/apache-beam-integration-testing/dicomStores/DICOM_store_2021-10-28_004230.486566_FR64u1FFOQ3mIN7", deidConfig));
    PAssert.that(result.getError().apply("toString", ToJson.of()))
        .containsInAnyOrder(Collections.emptyList());
    // PAssert.thatSingleton(result.getOperation().apply("output", Count.globally())).isEqualTo(1L);
    pipeline.run();
  }
}
