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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.beam.sdk.ml.inference.openai.OpenAIModelHandler.StructuredInputOutput;
import org.apache.beam.sdk.ml.inference.openai.OpenAIModelHandler.Response;
import org.apache.beam.sdk.ml.inference.remote.PredictionResult;



@RunWith(JUnit4.class)
public class OpenAIModelHandlerTest {
  private OpenAIModelParameters testParameters;

  @Before
  public void setUp() {
    testParameters = OpenAIModelParameters.builder()
      .apiKey("test-api-key")
      .modelName("gpt-4")
      .instructionPrompt("Test instruction")
      .build();
  }

  /**
   * Fake OpenAiModelHandler for testing.
   */
  static class FakeOpenAiModelHandler extends OpenAIModelHandler {

    private boolean clientCreated = false;
    private OpenAIModelParameters storedParameters;
    private List<OpenAIModelHandler.StructuredInputOutput> responsesToReturn;
    private RuntimeException exceptionToThrow;
    private boolean shouldReturnNull = false;

    public void setResponsesToReturn(List<StructuredInputOutput> responses) {
      this.responsesToReturn = responses;
    }

    public void setExceptionToThrow(RuntimeException exception) {
      this.exceptionToThrow = exception;
    }

    public void setShouldReturnNull(boolean shouldReturnNull) {
      this.shouldReturnNull = shouldReturnNull;
    }

    public boolean isClientCreated() {
      return clientCreated;
    }

    public OpenAIModelParameters getStoredParameters() {
      return storedParameters;
    }

    @Override
    public void createClient(OpenAIModelParameters parameters) {
      this.storedParameters = parameters;
      this.clientCreated = true;

      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
    }

    @Override
    public Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> request(
      List<OpenAIModelInput> input) {

      if (!clientCreated) {
        throw new IllegalStateException("Client not initialized");
      }

      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }

      if (shouldReturnNull || responsesToReturn == null) {
        throw new RuntimeException("Model returned no structured responses");
      }

      StructuredInputOutput structuredOutput = responsesToReturn.get(0);

      if (structuredOutput == null || structuredOutput.responses == null) {
        throw new RuntimeException("Model returned no structured responses");
      }

      return structuredOutput.responses.stream()
        .map(response -> PredictionResult.create(
          OpenAIModelInput.create(response.input),
          OpenAIModelResponse.create(response.output)))
        .collect(Collectors.toList());
    }
  }

  @Test
  public void testCreateClient() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    OpenAIModelParameters params = OpenAIModelParameters.builder()
      .apiKey("test-key")
      .modelName("gpt-4")
      .instructionPrompt("test prompt")
      .build();

    handler.createClient(params);

    assertTrue("Client should be created", handler.isClientCreated());
    assertNotNull("Parameters should be stored", handler.getStoredParameters());
    assertEquals("test-key", handler.getStoredParameters().getApiKey());
    assertEquals("gpt-4", handler.getStoredParameters().getModelName());
  }

  @Test
  public void testRequestWithSingleInput() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.singletonList(
      OpenAIModelInput.create("test input"));

    StructuredInputOutput structuredOutput = new StructuredInputOutput();
    Response response = new Response();
    response.input = "test input";
    response.output = "test output";
    structuredOutput.responses = Collections.singletonList(response);

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));
    handler.createClient(testParameters);

    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results = handler.request(inputs);

    assertNotNull("Results should not be null", results);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList = iterableToList(results);

    assertEquals("Should have 1 result", 1, resultList.size());

    PredictionResult<OpenAIModelInput, OpenAIModelResponse> result = resultList.get(0);
    assertEquals("test input", result.getInput().getModelInput());
    assertEquals("test output", result.getOutput().getModelResponse());
  }

  @Test
  public void testRequestWithMultipleInputs() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Arrays.asList(
      OpenAIModelInput.create("input1"),
      OpenAIModelInput.create("input2"),
      OpenAIModelInput.create("input3"));

    StructuredInputOutput structuredOutput = new StructuredInputOutput();

    Response response1 = new Response();
    response1.input = "input1";
    response1.output = "output1";

    Response response2 = new Response();
    response2.input = "input2";
    response2.output = "output2";

    Response response3 = new Response();
    response3.input = "input3";
    response3.output = "output3";

    structuredOutput.responses = Arrays.asList(response1, response2, response3);

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));
    handler.createClient(testParameters);

    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results = handler.request(inputs);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList = iterableToList(results);

    assertEquals("Should have 3 results", 3, resultList.size());

    for (int i = 0; i < 3; i++) {
      PredictionResult<OpenAIModelInput, OpenAIModelResponse> result = resultList.get(i);
      assertEquals("input" + (i + 1), result.getInput().getModelInput());
      assertEquals("output" + (i + 1), result.getOutput().getModelResponse());
    }
  }

  @Test
  public void testRequestWithEmptyInput() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.emptyList();

    StructuredInputOutput structuredOutput = new StructuredInputOutput();
    structuredOutput.responses = Collections.emptyList();

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));
    handler.createClient(testParameters);

    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results = handler.request(inputs);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList = iterableToList(results);
    assertEquals("Should have 0 results", 0, resultList.size());
  }

  @Test
  public void testRequestWithNullStructuredOutput() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.singletonList(
      OpenAIModelInput.create("test input"));

    handler.setShouldReturnNull(true);
    handler.createClient(testParameters);

    try {
      handler.request(inputs);
      fail("Expected RuntimeException when structured output is null");
    } catch (RuntimeException e) {
      assertTrue("Exception message should mention no structured responses",
        e.getMessage().contains("Model returned no structured responses"));
    }
  }

  @Test
  public void testRequestWithNullResponsesList() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.singletonList(
      OpenAIModelInput.create("test input"));

    StructuredInputOutput structuredOutput = new StructuredInputOutput();
    structuredOutput.responses = null;

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));
    handler.createClient(testParameters);

    try {
      handler.request(inputs);
      fail("Expected RuntimeException when responses list is null");
    } catch (RuntimeException e) {
      assertTrue("Exception message should mention no structured responses",
        e.getMessage().contains("Model returned no structured responses"));
    }
  }

  @Test
  public void testCreateClientFailure() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();
    handler.setExceptionToThrow(new RuntimeException("Setup failed"));

    try {
      handler.createClient(testParameters);
      fail("Expected RuntimeException during client creation");
    } catch (RuntimeException e) {
      assertEquals("Setup failed", e.getMessage());
    }
  }

  @Test
  public void testRequestApiFailure() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.singletonList(
      OpenAIModelInput.create("test input"));

    handler.createClient(testParameters);
    handler.setExceptionToThrow(new RuntimeException("API Error"));

    try {
      handler.request(inputs);
      fail("Expected RuntimeException when API fails");
    } catch (RuntimeException e) {
      assertEquals("API Error", e.getMessage());
    }
  }

  @Test
  public void testRequestWithoutClientInitialization() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Collections.singletonList(
      OpenAIModelInput.create("test input"));

    StructuredInputOutput structuredOutput = new StructuredInputOutput();
    Response response = new Response();
    response.input = "test input";
    response.output = "test output";
    structuredOutput.responses = Collections.singletonList(response);

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));

    // Don't call createClient
    try {
      handler.request(inputs);
      fail("Expected IllegalStateException when client not initialized");
    } catch (IllegalStateException e) {
      assertTrue("Exception should mention client not initialized",
        e.getMessage().contains("Client not initialized"));
    }
  }

  @Test
  public void testInputOutputMapping() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();

    List<OpenAIModelInput> inputs = Arrays.asList(
      OpenAIModelInput.create("alpha"),
      OpenAIModelInput.create("beta"));

    StructuredInputOutput structuredOutput = new StructuredInputOutput();

    Response response1 = new Response();
    response1.input = "alpha";
    response1.output = "ALPHA";

    Response response2 = new Response();
    response2.input = "beta";
    response2.output = "BETA";

    structuredOutput.responses = Arrays.asList(response1, response2);

    handler.setResponsesToReturn(Collections.singletonList(structuredOutput));
    handler.createClient(testParameters);

    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results = handler.request(inputs);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList = iterableToList(results);

    assertEquals(2, resultList.size());
    assertEquals("alpha", resultList.get(0).getInput().getModelInput());
    assertEquals("ALPHA", resultList.get(0).getOutput().getModelResponse());

    assertEquals("beta", resultList.get(1).getInput().getModelInput());
    assertEquals("BETA", resultList.get(1).getOutput().getModelResponse());
  }

  @Test
  public void testParametersBuilder() {
    OpenAIModelParameters params = OpenAIModelParameters.builder()
      .apiKey("my-api-key")
      .modelName("gpt-4-turbo")
      .instructionPrompt("Custom instruction")
      .build();

    assertEquals("my-api-key", params.getApiKey());
    assertEquals("gpt-4-turbo", params.getModelName());
    assertEquals("Custom instruction", params.getInstructionPrompt());
  }

  @Test
  public void testOpenAIModelInputCreate() {
    OpenAIModelInput input = OpenAIModelInput.create("test value");

    assertNotNull("Input should not be null", input);
    assertEquals("test value", input.getModelInput());
  }

  @Test
  public void testOpenAIModelResponseCreate() {
    OpenAIModelResponse response = OpenAIModelResponse.create("test output");

    assertNotNull("Response should not be null", response);
    assertEquals("test output", response.getModelResponse());
  }

  @Test
  public void testStructuredInputOutputStructure() {
    Response response = new Response();
    response.input = "test-input";
    response.output = "test-output";

    assertEquals("test-input", response.input);
    assertEquals("test-output", response.output);

    StructuredInputOutput structured = new StructuredInputOutput();
    structured.responses = Collections.singletonList(response);

    assertNotNull("Responses should not be null", structured.responses);
    assertEquals("Should have 1 response", 1, structured.responses.size());
    assertEquals("test-input", structured.responses.get(0).input);
  }

  @Test
  public void testMultipleRequestsWithSameHandler() {
    FakeOpenAiModelHandler handler = new FakeOpenAiModelHandler();
    handler.createClient(testParameters);

    // First request
    StructuredInputOutput output1 = new StructuredInputOutput();
    Response response1 = new Response();
    response1.input = "first";
    response1.output = "FIRST";
    output1.responses = Collections.singletonList(response1);
    handler.setResponsesToReturn(Collections.singletonList(output1));

    List<OpenAIModelInput> inputs1 = Collections.singletonList(
      OpenAIModelInput.create("first"));
    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results1 = handler.request(inputs1);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList1 = iterableToList(results1);
    assertEquals("FIRST", resultList1.get(0).getOutput().getModelResponse());

    // Second request with different data
    StructuredInputOutput output2 = new StructuredInputOutput();
    Response response2 = new Response();
    response2.input = "second";
    response2.output = "SECOND";
    output2.responses = Collections.singletonList(response2);
    handler.setResponsesToReturn(Collections.singletonList(output2));

    List<OpenAIModelInput> inputs2 = Collections.singletonList(
      OpenAIModelInput.create("second"));
    Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> results2 = handler.request(inputs2);

    List<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> resultList2 = iterableToList(results2);
    assertEquals("SECOND", resultList2.get(0).getOutput().getModelResponse());
  }

  // Helper method to convert Iterable to List
  private <T> List<T> iterableToList(Iterable<T> iterable) {
    List<T> list = new java.util.ArrayList<>();
    iterable.forEach(list::add);
    return list;
  }
}
