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

import com.google.gson.JsonParser;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.io.gcp.pubsub.*;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.*;
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

  @Rule public transient TestPubsubSignal signal = TestPubsubSignal.create();
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private final String pubsubTopic;
  private final String pubsubSubscription;
  private final String project;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String BASE_STORE_ID =
      "FHIR_store_search_it_" + System.currentTimeMillis() + "_" + (new SecureRandom().nextInt(32));
  private String fhirStoreId;
  private PubsubClient pubsub;
  private TestPubsubOptions pipelineOptions;

  public String version;

  public FhirIOSearchIT(String version) {
    this.version = version;
    long testTime = System.currentTimeMillis();
    this.fhirStoreId = BASE_STORE_ID + version;
    this.project =
        TestPipeline.testingPipelineOptions()
            .as(HealthcareStoreTestPipelineOptions.class)
            .getStoreProjectId();
    this.pubsubTopic =
        "projects/"
            + project
            + "/topics/FhirIO-IT-"
            + version
            + "-notifications-"
            + testTime
            + "-"
            + (new SecureRandom().nextInt(32));
    this.pubsubSubscription = pubsubTopic.replaceAll("topic", "subscription");
    pipelineOptions = TestPipeline.testingPipelineOptions().as(TestPubsubOptions.class);
  }

  @Before
  public void setup() throws Exception {
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    if (client == null) {
      this.client = new HttpHealthcareApiClient();
    }
    pubsub = PubsubGrpcClient.FACTORY.newClient(null, null, pipelineOptions);
    TopicPath topicPath = PubsubClient.topicPathFromPath(pubsubTopic);
    pubsub.createTopic(topicPath);
    SubscriptionPath subscriptionPath = PubsubClient.subscriptionPathFromPath(pubsubSubscription);
    pubsub.createSubscription(topicPath, subscriptionPath, 60);
    client.createFhirStore(healthcareDataset, fhirStoreId, version, pubsubTopic);

    // Execute bundles to trigger FHIR notifications to input topic.
    FhirIOTestUtil.executeFhirBundles(
        client,
        healthcareDataset + "/fhirStores/" + fhirStoreId,
        FhirIOTestUtil.BUNDLES.get(version));
  }

  @After
  public void deletePubsub() throws IOException {
    TopicPath topicPath = PubsubClient.topicPathFromPath(pubsubTopic);
    SubscriptionPath subscriptionPath = PubsubClient.subscriptionPathFromPath(pubsubSubscription);
    pubsub.deleteSubscription(subscriptionPath);
    pubsub.deleteTopic(topicPath);
    pubsub.close();
  }

  @AfterClass
  public static void teardown() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    for (String version : versions()) {
      client.deleteFhirStore(healthcareDataset + "/fhirStores/" + BASE_STORE_ID + version);
    }
  }

  @Test
  public void testFhirIOSearch() throws Exception {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);

    // Search using the resource type of each written resource and empty search parameters.
    FhirIO.Read.Result resources =
        pipeline
            .apply(PubsubIO.readStrings().fromSubscription(pubsubSubscription))
            .apply(FhirIO.readResources());
    HashMap<String, String> searchParameters = new HashMap<>();
    searchParameters.put("_count", Integer.toString(100)); // Ensure that pagination is tested
    PCollection<KV<String, Map<String, String>>> searchConfigs =
        resources
            .getResources()
            .apply(
                "ExtractResourceTypes",
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptors.strings(),
                            TypeDescriptors.maps(
                                TypeDescriptors.strings(), TypeDescriptors.strings())))
                    .via(
                        (String resource) ->
                            KV.of(
                                JsonParser.parseString(resource)
                                    .getAsJsonObject()
                                    .get("resourceType")
                                    .getAsString(),
                                searchParameters)));
    FhirIO.Search.Result result =
        searchConfigs.apply(
            FhirIO.searchResources(healthcareDataset + "/fhirStores/" + fhirStoreId));

    // Verify that there are no failures.
    PAssert.that(result.getFailedSearches()).empty();
    // Verify that none of the result resource sets are empty sets.
    PAssert.that(result.getResources())
        .satisfies(
            input -> {
              for (String resource : input) {
                assertNotEquals(
                    JsonParser.parseString(resource)
                        .getAsJsonObject()
                        .getAsJsonArray("entry")
                        .size(),
                    0);
              }
              return null;
            });

    pipeline.run().waitUntilFinish(Duration.standardMinutes(3));
  }
}
