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

import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.SearchResourcesRequest;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOReadIT {
  private FhirIOTestOptions options;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static long testTime = System.currentTimeMillis();
  private static final String FHIR_VERSION = "R4";
  private static final String FHIR_STORE_NAME =
      "FHIR_store_"
          + FHIR_VERSION
          + "_write_it_"
          + testTime
          + "_"
          + (new SecureRandom().nextInt(32));
  private static List<String> resourceIds = new ArrayList<>();

  @BeforeClass
  public static void createFHIRStore() throws IOException {
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
  }

  @Before
  public void setup() throws Exception {
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    client.createFhirStore(healthcareDataset, FHIR_STORE_NAME, FHIR_VERSION);
    FhirIOTestUtil.executeFhirBundles(
        client,
        healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME,
        FhirIOTestUtil.R4_PRETTY_BUNDLES);
    SearchResourcesRequest query = new SearchResourcesRequest();
    query.set("_summary", "id");
    HttpBody result =
        client.fhirSearch(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME, query);
    // TODO(jaketf) Get all FHIR IDs and set resourceIds
  }

  @After
  public void deleteFHIRtore() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteFhirStore(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME);
  }

  @Test
  public void testFhirIORead() {
    // Should read all messages.
    Pipeline pipeline = Pipeline.create();
    FhirIO.Read.Result result =
        pipeline
            .apply(Create.of(resourceIds).withCoder(StringUtf8Coder.of()))
            .apply(FhirIO.readResources());
    PCollection<Long> numReadMessages =
        result.getResources().setCoder(new HttpBodyCoder()).apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo((long) resourceIds.size());
    PAssert.that(result.getFailedReads()).empty();

    pipeline.run();
  }
}
