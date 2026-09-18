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
package org.apache.beam.sdk.ml.inference.gemini;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

import org.apache.beam.sdk.ml.inference.remote.PredictionResult;
import org.apache.beam.sdk.ml.inference.remote.RemoteInference;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Execute Gemini model handler integration test.
 *
 * <pre>
 * ./gradlew :sdks:java:ml:inference:gemini:integrationTest \
 *   --tests org.apache.beam.sdk.ml.inference.gemini.GeminiModelHandlerIT \
 *   --info
 * </pre>
 */
public class GeminiModelHandlerIT {

  public static class GeminiStringContentHandler
      extends GeminiModelHandler<GeminiStringInput, GeminiStringResponse> {}

  public static class GeminiStringImageHandler
      extends GeminiModelHandler<GeminiStringInput, GeminiImageResponse> {}

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private String apiKey;
  private static final String API_KEY_ENV = "GEMINI_API_KEY";
  private static final String DEFAULT_MODEL = "gemini-3.5-flash";

  @Before
  public void setUp() {
    // Get API key
    apiKey = System.getenv(API_KEY_ENV);

    // Skip tests if API key is not provided
    assumeNotNull(
        "Gemini API key not found. Set "
            + API_KEY_ENV
            + " environment variable to run integration tests.",
        apiKey);
    assumeTrue(
        "Gemini API key is empty. Set "
            + API_KEY_ENV
            + " environment variable to run integration tests.",
        !apiKey.trim().isEmpty());
  }

  @Test
  public void testSentimentAnalysisWithSingleInput() {
    String input =
        "Classify the sentiment of the following text as either positive or negative: This product is absolutely amazing! I love it!";

    PCollection<GeminiStringInput> inputs =
        pipeline.apply("CreateSingleInput", Create.of(new GeminiStringInput(input)));

    PCollection<Iterable<PredictionResult<GeminiStringInput, GeminiStringResponse>>> results =
        inputs.apply(
            "SentimentInference",
            RemoteInference.<GeminiStringInput, GeminiStringResponse>invoke()
                .handler(GeminiStringContentHandler.class)
                .withParameters(
                    GeminiModelParameters.<GeminiStringInput, GeminiStringResponse>builder()
                        .setApiKey(apiKey)
                        .setModelName(DEFAULT_MODEL)
                        .setRequestFn(GeminiInferenceFunctions.generateFromString())
                        .build()));

    // Verify results
    PAssert.that(results)
        .satisfies(
            batches -> {
              int count = 0;
              for (Iterable<PredictionResult<GeminiStringInput, GeminiStringResponse>> batch :
                  batches) {
                for (PredictionResult<GeminiStringInput, GeminiStringResponse> result : batch) {
                  count++;
                  assertNotNull("Input should not be null", result.getInput());
                  assertNotNull("Output should not be null", result.getOutput());

                  String sentiment = result.getOutput().getText().toLowerCase();
                  assertTrue(
                      "Sentiment should be positive or negative, got: " + sentiment,
                      sentiment.contains("positive") || sentiment.contains("negative"));
                }
              }
              assertEquals("Should have exactly 1 result", 1, count);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testGenerateImageFromString() {
    String input = "A beautiful sunset over the mountains, digital art style.";

    PCollection<GeminiStringInput> inputs =
        pipeline.apply("CreateImageInput", Create.of(new GeminiStringInput(input)));

    PCollection<Iterable<PredictionResult<GeminiStringInput, GeminiImageResponse>>> results =
        inputs.apply(
            "ImageInference",
            RemoteInference.<GeminiStringInput, GeminiImageResponse>invoke()
                .handler(GeminiStringImageHandler.class)
                .withParameters(
                    GeminiModelParameters.<GeminiStringInput, GeminiImageResponse>builder()
                        .setApiKey(apiKey)
                        .setModelName("imagen-4.0-generate-001")
                        .setRequestFn(GeminiInferenceFunctions.generateImageFromString())
                        .build()));

    // Verify results
    PAssert.that(results)
        .satisfies(
            batches -> {
              int count = 0;
              for (Iterable<PredictionResult<GeminiStringInput, GeminiImageResponse>> batch :
                  batches) {
                for (PredictionResult<GeminiStringInput, GeminiImageResponse> result : batch) {
                  count++;
                  assertNotNull("Input should not be null", result.getInput());
                  assertNotNull("Output should not be null", result.getOutput());
                  assertTrue(
                      "Output should have generated images",
                      result.getOutput().getImageBytes().length > 0);
                }
              }
              assertEquals("Should have exactly 1 result", 1, count);
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
