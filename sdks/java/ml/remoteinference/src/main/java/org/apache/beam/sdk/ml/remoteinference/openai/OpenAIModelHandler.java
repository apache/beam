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
package org.apache.beam.sdk.ml.remoteinference.openai;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonSchemaLocalValidation;
import com.openai.models.responses.ResponseCreateParams;
import com.openai.models.responses.StructuredResponseCreateParams;
import org.apache.beam.sdk.ml.remoteinference.base.BaseModelHandler;
import org.apache.beam.sdk.ml.remoteinference.base.PredictionResult;

import java.util.List;
import java.util.stream.Collectors;

public class OpenAIModelHandler
  implements BaseModelHandler<OpenAIModelParameters, OpenAIModelInput, OpenAIModelResponse> {

  private transient OpenAIClient client;
  private transient StructuredResponseCreateParams<StructuredInputOutput> clientParams;
  private OpenAIModelParameters modelParameters;

  @Override
  public void createClient(OpenAIModelParameters parameters) {
    this.modelParameters = parameters;
    this.client = OpenAIOkHttpClient.builder()
      .apiKey(this.modelParameters.getApiKey())
      .build();
  }

  @Override
  public Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> request(List<OpenAIModelInput> input) {

    try {
      // Convert input list to JSON string
      String inputBatch = new ObjectMapper()
        .writeValueAsString(input.stream().map(OpenAIModelInput::getModelInput).toList());

      // Build structured response parameters
      this.clientParams = ResponseCreateParams.builder()
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

      // Map responses to PredictionResults
      List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results = structuredOutput.responses.stream()
        .map(response -> PredictionResult.create(
          OpenAIModelInput.create(response.input),
          OpenAIModelResponse.create(response.output)))
        .collect(Collectors.toList());

      return results;

    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize input batch", e);
    }
  }

  public static class Response {
    @JsonProperty(required = true)
    @JsonPropertyDescription("The input string")
    public String input;

    @JsonProperty(required = true)
    @JsonPropertyDescription("The output string")
    public String output;
  }

  public static class StructuredInputOutput {
    @JsonProperty(required = true)
    @JsonPropertyDescription("Array of input-output pairs")
    public List<Response> responses;
  }

}
