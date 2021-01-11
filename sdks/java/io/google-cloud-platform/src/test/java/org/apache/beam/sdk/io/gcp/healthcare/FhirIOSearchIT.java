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
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class FhirIOSearchIT {

  @Parameters(name = "{0}")
  public static Collection<String> versions() {
    return Arrays.asList("R4");
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private final String project;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String BASE_STORE_ID =
      "FHIR_store_search_it_" + System.currentTimeMillis() + "_" + (new SecureRandom().nextInt(32));
  private String fhirStoreId;
  private static final int MAX_NUM_OF_SEARCHES = 50;
  private List<KV<String, Map<String, Object>>> input = new ArrayList<>();

  public String version;

  public FhirIOSearchIT(String version) {
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
    FhirIOTestUtil.executeFhirBundles(
        client, healthcareDataset + "/fhirStores/" + fhirStoreId, bundles);

    JsonArray fhirResources =
        JsonParser.parseString(bundles.get(0)).getAsJsonObject().getAsJsonArray("entry");
    HashMap<String, Object> searchParameters = new HashMap<>();
    searchParameters.put("_count", Integer.toString(100));
    int searches = 0;
    for (JsonElement resource : fhirResources) {
      input.add(
          KV.of(
              resource
                  .getAsJsonObject()
                  .getAsJsonObject("resource")
                  .get("resourceType")
                  .getAsString(),
              searchParameters));
      searches++;
      if (searches > MAX_NUM_OF_SEARCHES) {
        break;
      }
    }
  }

  @AfterClass
  public static void teardown() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    for (String version : versions()) {
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + BASE_STORE_ID + version);
    }
  }

  public static class StringObjectCoder extends CustomCoder<Object> {
    private static final StringObjectCoder CODER = new StringObjectCoder();
    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

    public static StringObjectCoder of() {
      return CODER;
    }

    @Override
    public void encode(Object value, OutputStream outStream) throws IOException {
      STRING_CODER.encode((String) value, outStream);
    }

    @Override
    public Object decode(InputStream inStream) throws IOException {
      return STRING_CODER.decode(inStream);
    }
  }

  @Test
  public void testFhirIOSearch() {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    // Search using the resource type of each written resource and empty search parameters.
    PCollection<KV<String, Map<String, Object>>> searchConfigs =
        pipeline.apply(
            Create.of(input)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(),
                        MapCoder.of(StringUtf8Coder.of(), StringObjectCoder.of()))));
    FhirIO.Search.Result result =
        searchConfigs.apply(
            FhirIO.searchResources(healthcareDataset + "/fhirStores/" + fhirStoreId));

    // Verify that there are no failures.
    PAssert.that(result.getFailedSearches()).empty();
    // Verify that none of the result resource sets are empty sets.
    PCollection<JsonArray> resources = result.getResources();
    PAssert.that(resources)
        .satisfies(
            input -> {
              for (JsonArray resource : input) {
                assertNotEquals(resource.size(), 0);
              }
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
