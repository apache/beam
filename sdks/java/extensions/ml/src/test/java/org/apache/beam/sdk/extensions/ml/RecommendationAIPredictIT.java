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

import static org.junit.Assert.assertTrue;

import com.google.api.client.json.GenericJson;
import com.google.cloud.recommendationengine.v1beta1.PredictResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RecommendationAIPredictIT {
  @Rule public TestPipeline testPipeline = TestPipeline.create();

  public static GenericJson getUserEvent() {
    GenericJson userInfo = new GenericJson().set("visitorId", "1");
    return new GenericJson().set("eventType", "home-page-view").set("userInfo", userInfo);
  }

  @Ignore("https://issues.apache.org/jira/browse/BEAM-12733")
  @Test
  public void predict() {
    String projectId = testPipeline.getOptions().as(GcpOptions.class).getProject();

    PCollectionTuple predictResult =
        testPipeline
            .apply(
                Create.of(Arrays.asList(getUserEvent()))
                    .withCoder(GenericJsonCoder.of(GenericJson.class)))
            .apply(
                RecommendationAIIO.predictAll()
                    .withProjectId(projectId)
                    .withPlacementId("recently_viewed_default"));
    PAssert.that(predictResult.get(RecommendationAIPredict.SUCCESS_TAG))
        .satisfies(new VerifyPredictResult());
    testPipeline.run().waitUntilFinish();
  }

  private static class VerifyPredictResult
      implements SerializableFunction<Iterable<PredictResponse.PredictionResult>, Void> {

    @Override
    public Void apply(Iterable<PredictResponse.PredictionResult> input) {
      List<PredictResponse.PredictionResult> matches = new ArrayList<>();
      input.forEach(
          item -> {
            PredictResponse.PredictionResult result = item;
            matches.add(result);
          });
      assertTrue(!matches.isEmpty());
      return null;
    }
  }
}
