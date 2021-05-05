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
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FhirIOPatchIT {

  public String version;

  @Parameters(name = "{0}")
  public static Collection<String> versions() {
    return Arrays.asList("R4");
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private final String project;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private String fhirStoreId;
  private String resourceName;
  private static final String BASE_STORE_ID =
      "FHIR_store_patch_it_" + System.currentTimeMillis() + "_" + (new SecureRandom().nextInt(32));

  public FhirIOPatchIT(String version) {
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

    resourceName = healthcareDataset + "/fhirStores/" + fhirStoreId + "/fhir/Patient/123";
    String bundle =
        "{\"resourceType\":\"Bundle\","
            + "\"type\":\"transaction\","
            + "\"entry\": [{"
            + "\"request\":{\"method\":\"PUT\",\"url\":\"Patient/123\"},"
            + "\"resource\":{\"resourceType\":\"Patient\",\"id\":\"123\",\"birthDate\": \"1990-01-01\"}"
            + "}]}";
    FhirIOTestUtil.executeFhirBundles(
        client, healthcareDataset + "/fhirStores/" + fhirStoreId, ImmutableList.of(bundle));
  }

  @After
  public void teardown() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    for (String version : versions()) {
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + BASE_STORE_ID + version);
    }
  }

  @Test
  public void testFhirIOPatch() throws IOException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    FhirPatchParameter patchParameter =
        FhirPatchParameter.builder()
            .setResourceName(resourceName)
            .setPatch(
                "[{\"op\": \"replace\", \"path\": \"/birthDate\", \"value\": \"1997-05-23\"}]")
            .build();
    String expectedSuccessBody = patchParameter.toString();

    // Execute patch.
    PCollection<FhirPatchParameter> patches =
        pipeline.apply(Create.of(patchParameter)); // .withCoder(FhirPatchParameterCoder.of()));
    FhirIO.Write.Result result = patches.apply(FhirIO.patchResources());

    // Validate beam results.
    PAssert.that(result.getFailedBodies()).empty();
    PCollection<String> successfulBodies = result.getSuccessfulBodies();
    PAssert.that(successfulBodies)
        .satisfies(
            input -> {
              for (String body : input) {
                Assert.assertEquals(expectedSuccessBody, body);
              }
              return null;
            });

    pipeline.run().waitUntilFinish();

    // Validate FHIR store contents.
    HttpBody readResult = client.readFhirResource(resourceName);
    Assert.assertEquals("1997-05-23", readResult.get("birthDate"));
  }

  @Test
  public void testFhirIOPatch_ifMatch() throws IOException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    FhirPatchParameter patchParameter =
        FhirPatchParameter.builder()
            .setResourceName(healthcareDataset + "/fhirStores/" + fhirStoreId + "/fhir/Patient")
            .setPatch(
                "[{\"op\": \"replace\", \"path\": \"/birthDate\", \"value\": \"1997-06-23\"}]")
            .setQuery(ImmutableMap.of("birthDate", "1990-01-01"))
            .build();
    String expectedSuccessBody = patchParameter.toString();

    // Execute patch.
    PCollection<FhirPatchParameter> patches =
        pipeline.apply(Create.of(patchParameter)); // .withCoder(FhirPatchParameterCoder.of()));
    FhirIO.Write.Result result = patches.apply(FhirIO.patchResources());

    // Validate beam results.
    PAssert.that(result.getFailedBodies()).empty();
    PCollection<String> successfulBodies = result.getSuccessfulBodies();
    PAssert.that(successfulBodies)
        .satisfies(
            input -> {
              for (String body : input) {
                Assert.assertEquals(expectedSuccessBody, body);
              }
              return null;
            });

    pipeline.run().waitUntilFinish();

    // Validate FHIR store contents.
    HttpBody readResult = client.readFhirResource(resourceName);
    Assert.assertEquals("1997-06-23", readResult.get("birthDate"));
  }
}
