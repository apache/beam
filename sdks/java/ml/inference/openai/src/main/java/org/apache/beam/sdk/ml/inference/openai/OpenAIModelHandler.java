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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonSchemaLocalValidation;
import com.openai.models.responses.ResponseCreateParams;
import com.openai.models.responses.StructuredResponseCreateParams;
import org.apache.beam.sdk.ml.inference.remote.BaseModelHandler;
import org.apache.beam.sdk.ml.inference.remote.PredictionResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Model handler for OpenAI API inference requests.
 *
 * <p>This handler manages communication with OpenAI's API, including client initialization,
 * request formatting, and response parsing. It uses OpenAI's structured output feature to
 * ensure reliable input-output pairing.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * OpenAIModelParameters params = OpenAIModelParameters.builder()
 *     .apiKey("sk-...")
 *     .modelName("gpt-4")
 *     .instructionPrompt("Classify the following text into one of the categories: {CATEGORIES}")
 *     .build();
 *
 * PCollection<OpenAIModelInput> inputs = ...;
 * PCollection<Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>>> results =
 *     inputs.apply(
 *         RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
 *             .handler(OpenAIModelHandler.class)
 *             .withParameters(params)
 *     );
 * }</pre>
 *
 */
public class OpenAIModelHandler
  implements BaseModelHandler<OpenAIModelParameters, OpenAIModelInput, OpenAIModelResponse> {

  private transient OpenAIClient client;
  private OpenAIModelParameters modelParameters;

  /**
   * Initializes the OpenAI client with the provided parameters.
   *
   * <p>This method is called once during setup. It creates an authenticated
   * OpenAI client using the API key from the parameters.
   *
   * @param parameters the configuration parameters including API key and model name
   */
  @Override
  public void createClient(OpenAIModelParameters parameters) {
    this.modelParameters = parameters;
    this.client = OpenAIOkHttpClient.builder()
      .apiKey(this.modelParameters.getApiKey())
      .build();
  }

  /**
   * Performs inference on a batch of inputs using the OpenAI Client.
   *
   * <p>This method serializes the input batch to JSON string, sends it to OpenAI with structured
   * output requirements, and parses the response into {@link PredictionResult} objects
   * that pair each input with its corresponding output.
   *
   * @param input the list of inputs to process
   * @return an iterable of model results and input pairs
   */
  @Override
  public Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> request(List<OpenAIModelInput> input) {

    try {
      // Convert input list to JSON string
      String inputBatch = new ObjectMapper()
        .writeValueAsString(input.stream().map(OpenAIModelInput::getModelInput).toList());

      // Build structured response parameters
      StructuredResponseCreateParams<StructuredInputOutput> clientParams = ResponseCreateParams.builder()
        .model(modelParameters.getModelName())
        .input(inputBatch)
        .text(StructuredInputOutput.class, JsonSchemaLocalValidation.NO)
        .instructions(modelParameters.getInstructionPrompt())
        .build();

      // Get structured output from the model
      StructuredInputOutput structuredOutput = client.responses()
        .create(clientParams)
        .output()
        .stream()
        .flatMap(item -> item.message().stream())
        .flatMap(message -> message.content().stream())
        .flatMap(content -> content.outputText().stream())
        .findFirst()
        .orElse(null);

      if (structuredOutput == null || structuredOutput.responses == null) {
        throw new RuntimeException("Model returned no structured responses");
      }

      // return PredictionResults
      return structuredOutput.responses.stream()
        .map(response -> PredictionResult.create(
          OpenAIModelInput.create(response.input),
          OpenAIModelResponse.create(response.output)))
        .collect(Collectors.toList());

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize input batch", e);
    }
  }

  /**
   * Schema class for structured output response.
   *
   * <p>Represents a single input-output pair returned by the OpenAI API.
   */
  public static class Response {
    @JsonProperty(required = true)
    @JsonPropertyDescription("The input string")
    public String input;

    @JsonProperty(required = true)
    @JsonPropertyDescription("The output string")
    public String output;
  }

  /**
   * Schema class for structured output containing multiple responses.
   *
   * <p>This class defines the expected JSON structure for OpenAI's structured output,
   * ensuring reliable parsing of batched inference results.
   */
  public static class StructuredInputOutput {
    @JsonProperty(required = true)
    @JsonPropertyDescription("Array of input-output pairs")
    public List<Response> responses;
  }

}
