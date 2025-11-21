/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.ml.inference.openai;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.ml.inference.remote.RemoteInference;
import org.apache.beam.sdk.ml.inference.remote.PredictionResult;

public class OpenAIModelHandlerIT {
  private static final Logger LOG = LoggerFactory.getLogger(OpenAIModelHandlerIT.class);

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private String apiKey;
  private static final String API_KEY_ENV = "OPENAI_API_KEY";
  private static final String DEFAULT_MODEL = "gpt-4o-mini";


  @Before
  public void setUp() {
    // Get API key
    apiKey = System.getenv(API_KEY_ENV);

    // Skip tests if API key is not provided
    assumeNotNull(
      "OpenAI API key not found. Set " + API_KEY_ENV
        + " environment variable to run integration tests.",
      apiKey);
    assumeTrue("OpenAI API key is empty. Set " + API_KEY_ENV
        + " environment variable to run integration tests.",
      !apiKey.trim().isEmpty());
  }

  @Test
  public void testSentimentAnalysisWithSingleInput() {
    String input = "This product is absolutely amazing! I love it!";

    PCollection<OpenAIModelInput> inputs = pipeline
      .apply("CreateSingleInput", Create.of(input))
      .apply("MapToInput", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputs
      .apply("SentimentInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName(DEFAULT_MODEL)
            .instructionPrompt(
              "Analyze the sentiment as 'positive' or 'negative'. Return only one word.")
            .build()));

    // Verify results
    PAssert.that(results).satisfies(batches -> {
      int count = 0;
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          count++;
          assertNotNull("Input should not be null", result.getInput());
          assertNotNull("Output should not be null", result.getOutput());
          assertNotNull("Output text should not be null",
            result.getOutput().getModelResponse());

          String sentiment = result.getOutput().getModelResponse().toLowerCase();
          assertTrue("Sentiment should be positive or negative, got: " + sentiment,
            sentiment.contains("positive")
              || sentiment.contains("negative"));
        }
      }
      assertEquals("Should have exactly 1 result", 1, count);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSentimentAnalysisWithMultipleInputs() {
    List<String> inputs = Arrays.asList(
      "An excellent B2B SaaS solution that streamlines business processes efficiently.",
      "The customer support is terrible. I've been waiting for days without any response.",
      "The application works as expected. Installation was straightforward.",
      "Really impressed with the innovative features! The AI capabilities are groundbreaking!",
      "Mediocre product with occasional glitches. Documentation could be better.");

    PCollection<OpenAIModelInput> inputCollection = pipeline
      .apply("CreateMultipleInputs", Create.of(inputs))
      .apply("MapToInputs", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputCollection
      .apply("SentimentInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName(DEFAULT_MODEL)
            .instructionPrompt(
              "Analyze sentiment as positive or negative")
            .build()));

    // Verify we get results for all inputs
    PAssert.that(results).satisfies(batches -> {
      int totalCount = 0;
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          totalCount++;
          assertNotNull("Input should not be null", result.getInput());
          assertNotNull("Output should not be null", result.getOutput());
          assertFalse("Output should not be empty",
            result.getOutput().getModelResponse().trim().isEmpty());
        }
      }
      assertEquals("Should have results for all 5 inputs", 5, totalCount);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTextClassification() {
    List<String> inputs = Arrays.asList(
      "How do I reset my password?",
      "Your product is broken and I want a refund!",
      "Thank you for the excellent service!");

    PCollection<OpenAIModelInput> inputCollection = pipeline
      .apply("CreateInputs", Create.of(inputs))
      .apply("MapToInputs", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputCollection
      .apply("ClassificationInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName(DEFAULT_MODEL)
            .instructionPrompt(
              "Classify each text into one category: 'question', 'complaint', or 'praise'. Return only the category.")
            .build()));

    PAssert.that(results).satisfies(batches -> {
      List<String> categories = new ArrayList<>();
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          String category = result.getOutput().getModelResponse().toLowerCase();
          categories.add(category);
        }
      }

      assertEquals("Should have 3 categories", 3, categories.size());

      // Verify expected categories
      boolean hasQuestion = categories.stream().anyMatch(c -> c.contains("question"));
      boolean hasComplaint = categories.stream().anyMatch(c -> c.contains("complaint"));
      boolean hasPraise = categories.stream().anyMatch(c -> c.contains("praise"));

      assertTrue("Should have at least one recognized category",
        hasQuestion || hasComplaint || hasPraise);

      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testInputOutputMapping() {
    List<String> inputs = Arrays.asList("apple", "banana", "cherry");

    PCollection<OpenAIModelInput> inputCollection = pipeline
      .apply("CreateInputs", Create.of(inputs))
      .apply("MapToInputs", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputCollection
      .apply("MappingInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName(DEFAULT_MODEL)
            .instructionPrompt(
              "Return the input word in uppercase")
            .build()));

    // Verify input-output pairing is preserved
    PAssert.that(results).satisfies(batches -> {
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          String input = result.getInput().getModelInput();
          String output = result.getOutput().getModelResponse().toLowerCase();

          // Verify the output relates to the input
          assertTrue("Output should relate to input '" + input + "', got: " + output,
            output.contains(input.toLowerCase()));
        }
      }
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWithDifferentModel() {
    // Test with a different model
    String input = "Explain quantum computing in one sentence.";

    PCollection<OpenAIModelInput> inputs = pipeline
      .apply("CreateInput", Create.of(input))
      .apply("MapToInput", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputs
      .apply("DifferentModelInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName("gpt-5")
            .instructionPrompt("Respond concisely")
            .build()));

    PAssert.that(results).satisfies(batches -> {
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          assertNotNull("Output should not be null",
            result.getOutput().getModelResponse());
          assertFalse("Output should not be empty",
            result.getOutput().getModelResponse().trim().isEmpty());
        }
      }
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWithInvalidApiKey() {
    String input = "Test input";

    PCollection<OpenAIModelInput> inputs = pipeline
      .apply("CreateInput", Create.of(input))
      .apply("MapToInput", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    inputs.apply("InvalidKeyInference",
      RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
        .handler(OpenAIModelHandler.class)
        .withParameters(OpenAIModelParameters.builder()
          .apiKey("invalid-api-key-12345")
          .modelName(DEFAULT_MODEL)
          .instructionPrompt("Test")
          .build()));

    try {
      pipeline.run().waitUntilFinish();
      fail("Expected pipeline failure due to invalid API key");
    } catch (Exception e) {
      String msg = e.toString().toLowerCase();

      assertTrue(
        "Expected retry exhaustion or API key issue. Got: " + msg,
        msg.contains("exhaust") ||
          msg.contains("max retries") ||
          msg.contains("401") ||
          msg.contains("api key") ||
          msg.contains("incorrect api key")
      );
    }
  }

  /**
   * Test with custom instruction formats
   */
  @Test
  public void testWithJsonOutputFormat() {
    String input = "Paris is the capital of France";

    PCollection<OpenAIModelInput> inputs = pipeline
      .apply("CreateInput", Create.of(input))
      .apply("MapToInput", MapElements
        .into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create));

    PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results = inputs
      .apply("JsonFormatInference",
        RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
          .handler(OpenAIModelHandler.class)
          .withParameters(OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName(DEFAULT_MODEL)
            .instructionPrompt(
              "Extract the city and country. Return as: City: [city], Country: [country]")
            .build()));

    PAssert.that(results).satisfies(batches -> {
      for (Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> batch : batches) {
        for (PredictionResult<OpenAIModelInput, OpenAIModelResponse> result : batch) {
          String output = result.getOutput().getModelResponse();
          LOG.info("Structured output: " + output);

          // Verify output contains expected information
          assertTrue("Output should mention Paris: " + output,
            output.toLowerCase().contains("paris"));
          assertTrue("Output should mention France: " + output,
            output.toLowerCase().contains("france"));
        }
      }
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testRetryWithInvalidModel() {

    PCollection<OpenAIModelInput> inputs =
      pipeline
        .apply("CreateInput", Create.of("Test input"))
        .apply("MapToInput",
          MapElements.into(TypeDescriptor.of(OpenAIModelInput.class))
            .via(OpenAIModelInput::create));

    inputs.apply(
      "FailingOpenAIRequest",
      RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
        .handler(OpenAIModelHandler.class)
        .withParameters(
          OpenAIModelParameters.builder()
            .apiKey(apiKey)
            .modelName("fake-model")
            .instructionPrompt("test retry")
            .build()));

    try {
      pipeline.run().waitUntilFinish();
      fail("Pipeline should fail after retry exhaustion.");
    } catch (Exception e) {
      String message = e.getMessage().toLowerCase();

      assertTrue(
        "Expected retry-exhaustion error. Actual: " + message,
        message.contains("exhaust") ||
          message.contains("retry") ||
          message.contains("max retries") ||
          message.contains("request failed") ||
          message.contains("fake-model"));
    }
  }

}
