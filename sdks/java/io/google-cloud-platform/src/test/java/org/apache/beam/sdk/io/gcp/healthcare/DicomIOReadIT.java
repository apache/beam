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

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({"nullness", "rawtypes", "uninitialized"})
public class DicomIOReadIT {
  private static final String TEST_FILE_PATH = "src/test/resources/DICOM/testDicomFile.dcm";
  private static final String TEST_FILE_STUDY_ID = "study_000000000";
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private String healthcareDataset;
  private String project;
  private HealthcareApiClient client;
  private String storeName = "foo";

  @Before
  public void setup() throws IOException, URISyntaxException {
    project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    client = new HttpHealthcareApiClient();

    client.createDicomStore(healthcareDataset, storeName);
    client.uploadToDicomStore(healthcareDataset + "/dicomStores/" + storeName, TEST_FILE_PATH);
  }

  @After
  public void deleteDicomStore() throws IOException {
    client.deleteDicomStore(healthcareDataset + "/dicomStores/" + storeName);
  }

  @Ignore("https://jira.apache.org/jira/browse/BEAM-11376")
  @Test
  public void testDicomMetadataRead() throws IOException {
    String webPath =
        String.format(
            "%s/dicomStores/%s/dicomWeb/studies/%s",
            healthcareDataset, storeName, TEST_FILE_STUDY_ID);

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
}
