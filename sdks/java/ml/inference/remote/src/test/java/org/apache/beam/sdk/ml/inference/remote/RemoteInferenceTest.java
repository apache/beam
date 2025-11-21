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
package org.apache.beam.sdk.ml.inference.remote;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;



@RunWith(JUnit4.class)
public class RemoteInferenceTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  // Test input class
  public static class TestInput extends BaseInput {
    private final String value;

    private TestInput(String value) {
      this.value = value;
    }

    public static TestInput create(String value) {
      return new TestInput(value);
    }

    public String getModelInput() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof TestInput))
        return false;
      TestInput testInput = (TestInput) o;
      return value.equals(testInput.value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public String toString() {
      return "TestInput{value='" + value + "'}";
    }
  }

  // Test output class
  public static class TestOutput extends BaseResponse {
    private final String result;

    private TestOutput(String result) {
      this.result = result;
    }

    public static TestOutput create(String result) {
      return new TestOutput(result);
    }

    public String getModelResponse() {
      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof TestOutput))
        return false;
      TestOutput that = (TestOutput) o;
      return result.equals(that.result);
    }

    @Override
    public int hashCode() {
      return result.hashCode();
    }

    @Override
    public String toString() {
      return "TestOutput{result='" + result + "'}";
    }
  }

  // Test parameters class
  public static class TestParameters implements BaseModelParameters {
    private final String config;

    private TestParameters(Builder builder) {
      this.config = builder.config;
    }

    public String getConfig() {
      return config;
    }

    @Override
    public String toString() {
      return "TestParameters{config='" + config + "'}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof TestParameters))
        return false;
      TestParameters that = (TestParameters) o;
      return config.equals(that.config);
    }

    @Override
    public int hashCode() {
      return config.hashCode();
    }

    // Builder
    public static class Builder {
      private String config;

      public Builder setConfig(String config) {
        this.config = config;
        return this;
      }

      public TestParameters build() {
        return new TestParameters(this);
      }
    }

    public static Builder builder() {
      return new Builder();
    }
  }

  // Mock handler for successful inference
  public static class MockSuccessHandler
    implements BaseModelHandler<TestParameters, TestInput, TestOutput> {

    private TestParameters parameters;
    private boolean clientCreated = false;

    @Override
    public void createClient(TestParameters parameters) {
      this.parameters = parameters;
      this.clientCreated = true;
    }

    @Override
    public Iterable<PredictionResult<TestInput, TestOutput>> request(List<TestInput> input) {
      if (!clientCreated) {
        throw new IllegalStateException("Client not initialized");
      }
      return input.stream()
        .map(i -> PredictionResult.create(
          i,
          new TestOutput("processed-" + i.getModelInput())))
        .collect(Collectors.toList());
    }
  }

  // Mock handler that returns empty results
  public static class MockEmptyResultHandler
    implements BaseModelHandler<TestParameters, TestInput, TestOutput> {

    @Override
    public void createClient(TestParameters parameters) {
      // Setup succeeds
    }

    @Override
    public Iterable<PredictionResult<TestInput, TestOutput>> request(List<TestInput> input) {
      return Collections.emptyList();
    }
  }

  // Mock handler that throws exception during setup
  public static class MockFailingSetupHandler
    implements BaseModelHandler<TestParameters, TestInput, TestOutput> {

    @Override
    public void createClient(TestParameters parameters) {
      throw new RuntimeException("Setup failed intentionally");
    }

    @Override
    public Iterable<PredictionResult<TestInput, TestOutput>> request(List<TestInput> input) {
      return Collections.emptyList();
    }
  }

  // Mock handler that throws exception during request
  public static class MockFailingRequestHandler
    implements BaseModelHandler<TestParameters, TestInput, TestOutput> {

    @Override
    public void createClient(TestParameters parameters) {
      // Setup succeeds
    }

    @Override
    public Iterable<PredictionResult<TestInput, TestOutput>> request(List<TestInput> input) {
      throw new RuntimeException("Request failed intentionally");
    }
  }

  // Mock handler without default constructor (to test error handling)
  public static class MockNoDefaultConstructorHandler
    implements BaseModelHandler<TestParameters, TestInput, TestOutput> {

    private final String required;

    public MockNoDefaultConstructorHandler(String required) {
      this.required = required;
    }

    @Override
    public void createClient(TestParameters parameters) {
    }

    @Override
    public Iterable<PredictionResult<TestInput, TestOutput>> request(List<TestInput> input) {
      return Collections.emptyList();
    }
  }

  private static boolean containsMessage(Throwable e, String message) {
    Throwable current = e;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(message)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Test
  public void testInvokeWithSingleElement() {
    TestInput input = TestInput.create("test-value");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline.apply(Create.of(input));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)
          .withParameters(params));

    // Verify the output contains expected predictions
    PAssert.thatSingleton(results).satisfies(batch -> {
      List<PredictionResult<TestInput, TestOutput>> resultList = StreamSupport.stream(batch.spliterator(), false)
        .collect(Collectors.toList());

      assertEquals("Expected exactly 1 result", 1, resultList.size());

      PredictionResult<TestInput, TestOutput> result = resultList.get(0);
      assertEquals("test-value", result.getInput().getModelInput());
      assertEquals("processed-test-value", result.getOutput().getModelResponse());

      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testInvokeWithMultipleElements() {
    List<TestInput> inputs = Arrays.asList(
      new TestInput("input1"),
      new TestInput("input2"),
      new TestInput("input3"));

    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInputs", Create.of(inputs).withCoder(SerializableCoder.of(TestInput.class)));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)
          .withParameters(params));

    // Count total results across all batches
    PAssert.that(results).satisfies(batches -> {
      int totalCount = 0;
      for (Iterable<PredictionResult<TestInput, TestOutput>> batch : batches) {
        for (PredictionResult<TestInput, TestOutput> result : batch) {
          totalCount++;
          assertTrue("Output should start with 'processed-'",
            result.getOutput().getModelResponse().startsWith("processed-"));
          assertNotNull("Input should not be null", result.getInput());
          assertNotNull("Output should not be null", result.getOutput());
        }
      }
      assertEquals("Expected 3 total results", 3, totalCount);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testInvokeWithEmptyCollection() {
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateEmptyInput", Create.empty(SerializableCoder.of(TestInput.class)));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)
          .withParameters(params));

    // assertion for empty PCollection
    PAssert.that(results).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testHandlerReturnsEmptyResults() {
    TestInput input = new TestInput("test-value");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInput", Create.of(input).withCoder(SerializableCoder.of(TestInput.class)));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockEmptyResultHandler.class)
          .withParameters(params));

    // Verify we still get a result, but it's empty
    PAssert.thatSingleton(results).satisfies(batch -> {
      List<PredictionResult<TestInput, TestOutput>> resultList = StreamSupport.stream(batch.spliterator(), false)
        .collect(Collectors.toList());
      assertEquals("Expected empty result list", 0, resultList.size());
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testHandlerSetupFailure() {
    TestInput input = new TestInput("test-value");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInput", Create.of(input).withCoder(SerializableCoder.of(TestInput.class)));

    inputCollection.apply("RemoteInference",
      RemoteInference.<TestInput, TestOutput>invoke()
        .handler(MockFailingSetupHandler.class)
        .withParameters(params));

    // Verify pipeline fails with expected error
    try {
      pipeline.run().waitUntilFinish();
      fail("Expected pipeline to fail due to handler setup failure");
    } catch (Exception e) {
      String message = e.getMessage();
      assertTrue("Exception should mention setup failure or handler instantiation failure",
        message != null && (message.contains("Setup failed intentionally") ||
          message.contains("Failed to instantiate handler")));
    }
  }

  @Test
  public void testHandlerRequestFailure() {
    TestInput input = new TestInput("test-value");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInput", Create.of(input).withCoder(SerializableCoder.of(TestInput.class)));

    inputCollection.apply("RemoteInference",
      RemoteInference.<TestInput, TestOutput>invoke()
        .handler(MockFailingRequestHandler.class)
        .withParameters(params));

    // Verify pipeline fails with expected error
    try {
      pipeline.run().waitUntilFinish();
      fail("Expected pipeline to fail due to request failure");
    } catch (Exception e) {

      assertTrue(
        "Expected 'Request failed intentionally' in exception chain",
        containsMessage(e, "Request failed intentionally"));
    }
  }

  @Test
  public void testHandlerWithoutDefaultConstructor() {
    TestInput input = new TestInput("test-value");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInput", Create.of(input).withCoder(SerializableCoder.of(TestInput.class)));

    inputCollection.apply("RemoteInference",
      RemoteInference.<TestInput, TestOutput>invoke()
        .handler(MockNoDefaultConstructorHandler.class)
        .withParameters(params));

    // Verify pipeline fails when handler cannot be instantiated
    try {
      pipeline.run().waitUntilFinish();
      fail("Expected pipeline to fail due to missing default constructor");
    } catch (Exception e) {
      String message = e.getMessage();
      assertTrue("Exception should mention handler instantiation failure",
        message != null && message.contains("Failed to instantiate handler"));
    }
  }

  @Test
  public void testBuilderPattern() {
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    RemoteInference.Invoke<TestInput, TestOutput> transform = RemoteInference.<TestInput, TestOutput>invoke()
      .handler(MockSuccessHandler.class)
      .withParameters(params);

    assertNotNull("Transform should not be null", transform);
  }

  @Test
  public void testPredictionResultMapping() {
    TestInput input = new TestInput("mapping-test");
    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInput", Create.of(input).withCoder(SerializableCoder.of(TestInput.class)));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)
          .withParameters(params));

    PAssert.thatSingleton(results).satisfies(batch -> {
      for (PredictionResult<TestInput, TestOutput> result : batch) {
        // Verify that input is preserved in the result
        assertNotNull("Input should not be null", result.getInput());
        assertNotNull("Output should not be null", result.getOutput());
        assertEquals("mapping-test", result.getInput().getModelInput());
        assertTrue("Output should contain input value",
          result.getOutput().getModelResponse().contains("mapping-test"));
      }
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  // Temporary behaviour until we introduce java BatchElements transform
  // to batch elements in RemoteInference
  @Test
  public void testMultipleInputsProduceSeparateBatches() {
    List<TestInput> inputs = Arrays.asList(
      new TestInput("input1"),
      new TestInput("input2"));

    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    PCollection<TestInput> inputCollection = pipeline
      .apply("CreateInputs", Create.of(inputs).withCoder(SerializableCoder.of(TestInput.class)));

    PCollection<Iterable<PredictionResult<TestInput, TestOutput>>> results = inputCollection
      .apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)
          .withParameters(params));

    PAssert.that(results).satisfies(batches -> {
      int batchCount = 0;
      for (Iterable<PredictionResult<TestInput, TestOutput>> batch : batches) {
        batchCount++;
        int elementCount = 0;
        elementCount += StreamSupport.stream(batch.spliterator(), false).count();
        // Each batch should contain exactly 1 element
        assertEquals("Each batch should contain 1 element", 1, elementCount);
      }
      assertEquals("Expected 2 batches", 2, batchCount);
      return null;
    });

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWithEmptyParameters() {

    pipeline.enableAbandonedNodeEnforcement(false);

    TestInput input = TestInput.create("test-value");
    PCollection<TestInput> inputCollection = pipeline.apply(Create.of(input));

    IllegalArgumentException thrown = assertThrows(
      IllegalArgumentException.class,
      () -> inputCollection.apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .handler(MockSuccessHandler.class)));

    assertTrue(
      "Expected message to contain 'withParameters() is required', but got: " + thrown.getMessage(),
      thrown.getMessage().contains("withParameters() is required"));
  }

  @Test
  public void testWithEmptyHandler() {

    pipeline.enableAbandonedNodeEnforcement(false);

    TestParameters params = TestParameters.builder()
      .setConfig("test-config")
      .build();

    TestInput input = TestInput.create("test-value");
    PCollection<TestInput> inputCollection = pipeline.apply(Create.of(input));

    IllegalArgumentException thrown = assertThrows(
      IllegalArgumentException.class,
      () -> inputCollection.apply("RemoteInference",
        RemoteInference.<TestInput, TestOutput>invoke()
          .withParameters(params)));

    assertTrue(
      "Expected message to contain 'handler() is required', but got: " + thrown.getMessage(),
      thrown.getMessage().contains("handler() is required"));
  }
}
