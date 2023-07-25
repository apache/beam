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
import static org.junit.Assert.assertNotEquals;

import com.google.gson.JsonArray;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOPatientEverything.PatientEverythingParameter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FhirIOPatientEverythingIT {

  public String version;
  private final String project;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String BASE_STORE_ID =
      "FHIR_store_patient_everything_it_"
          + System.currentTimeMillis()
          + "_"
          + new SecureRandom().nextInt(32);
  private String fhirStoreId;

  private List<PatientEverythingParameter> input = new ArrayList<>();

  @Parameters(name = "{0}")
  public static Collection<String> versions() {
    return Arrays.asList("R4");
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  public FhirIOPatientEverythingIT(String version) {
    this.version = version;
    this.fhirStoreId = BASE_STORE_ID + version;
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
    client.createFhirStore(healthcareDataset, fhirStoreId, version, "");

    List<String> bundles = FhirIOTestUtil.BUNDLES.get(version);
    List<String> prepopulatedResourceNames =
        FhirIOTestUtil.executeFhirBundles(
            client, healthcareDataset + "/fhirStores/" + fhirStoreId, bundles);

    HashMap<String, String> filters = new HashMap<>();
    // filters.put("_count", Integer.toString(50));

    int requests = 0;
    for (String resourceName : prepopulatedResourceNames) {
      // Skip non-patient resource types.
      if (!resourceName.contains("/fhir/Patient/")) {
        continue;
      }
      input.add(
          PatientEverythingParameter.builder()
              .setResourceName(resourceName)
              .setFilters(filters)
              .build());

      requests++;
      if (requests > 50) {
        break;
      }
    }
  }

  @After
  public void teardown() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    for (String version : versions()) {
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + BASE_STORE_ID + version);
    }
  }

  @Test
  public void testFhirIOPatientEverything() {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    PCollection<PatientEverythingParameter> everythingConfigs = pipeline.apply(Create.of(input));
    FhirIOPatientEverything.Result result = everythingConfigs.apply(FhirIO.getPatientEverything());

    // Verify that there are no failures.
    PAssert.that(result.getFailedReads()).empty();
    // Verify that none of the result resource sets are empty sets.
    PCollection<JsonArray> resources = result.getPatientCompartments();
    PAssert.that(resources)
        .satisfies(
            input -> {
              for (JsonArray resource : input) {
                assertNotEquals(0, resource.size());
              }
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
