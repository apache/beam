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
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({"nullness", "rawtypes", "uninitialized"})
public class DicomIOReadIT {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private String healthcareDataset;
  private String project;
  private HealthcareApiClient client;
  private String storeName = "foo";

  @Before
  public void setup() throws IOException {
    project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    client = new HttpHealthcareApiClient();

    client.createDicomStore(healthcareDataset, storeName);
  }

  @After
  public void deleteDicomStore() throws IOException {
    client.deleteDicomStore(healthcareDataset + "/dicomStores/" + storeName);
  }

  @Test
  public void testDicomMetadataRead() throws IOException {
    String webPath =
        String.format("%s/dicomStores/%s/dicomWeb/studies/", healthcareDataset, storeName);

    DicomIO.ReadStudyMetadata.Result result =
        pipeline.apply(Create.of(webPath)).apply(DicomIO.readStudyMetadata());

    PipelineResult job = pipeline.run();

    try {
      job.cancel();
    } catch (UnsupportedOperationException exc) {
      // noop - if runner does not support job.cancel()
    }
  }
}
