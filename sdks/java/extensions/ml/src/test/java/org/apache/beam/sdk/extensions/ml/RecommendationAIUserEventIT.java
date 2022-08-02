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
package org.apache.beam.sdk.extensions.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.client.json.GenericJson;
import com.google.cloud.recommendationengine.v1beta1.UserEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RecommendationAIUserEventIT {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  public static GenericJson getUserEvent() {
    GenericJson userInfo = new GenericJson().set("visitorId", "1");
    GenericJson productDetail = new GenericJson().set("id", "1").set("quantity", 1);
    ArrayList<GenericJson> productDetails = new ArrayList<>();
    productDetails.add(productDetail);
    GenericJson productEventDetail = new GenericJson().set("productDetails", productDetails);
    return new GenericJson()
        .set("eventType", "detail-page-view")
        .set("userInfo", userInfo)
        .set("productEventDetail", productEventDetail);
  }

  @Test
  public void createUserEvent() {
    String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();

    PCollectionTuple createUserEventResult =
        testPipeline
            .apply(
                Create.of(Arrays.asList(getUserEvent()))
                    .withCoder(GenericJsonCoder.of(GenericJson.class)))
            .apply(RecommendationAIIO.writeUserEvent().withProjectId(projectId));
    PAssert.that(createUserEventResult.get(RecommendationAIWriteUserEvent.SUCCESS_TAG))
        .satisfies(new VerifyUserEventResult(1));
    testPipeline.run().waitUntilFinish();
  }

  @Ignore("Import method causing issues")
  @Test
  public void importUserEvents() {
    String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();
    ArrayList<KV<String, GenericJson>> userEvents = new ArrayList<>();
    userEvents.add(KV.of("123", getUserEvent()));
    userEvents.add(KV.of("123", getUserEvent()));

    PCollectionTuple importUserEventResult =
        testPipeline
            .apply(Create.of(userEvents))
            .apply(RecommendationAIImportUserEvents.newBuilder().setProjectId(projectId).build());
    PAssert.that(importUserEventResult.get(RecommendationAIWriteUserEvent.SUCCESS_TAG))
        .satisfies(new VerifyUserEventResult(2));
    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyUserEventResult
      implements SerializableFunction<Iterable<UserEvent>, Void> {

    int size;

    private VerifyUserEventResult(int size) {
      this.size = size;
    }

    @Override
    public Void apply(Iterable<UserEvent> input) {
      List<String> matches = new ArrayList<>();
      input.forEach(
          item -> {
            UserEvent result = item;
            matches.add(result.getUserInfo().getVisitorId());
          });
      assertTrue(matches.contains(((GenericJson) getUserEvent().get("userInfo")).get("visitorId")));
      assertEquals(size, matches.size());
      return null;
    }
  }
}
