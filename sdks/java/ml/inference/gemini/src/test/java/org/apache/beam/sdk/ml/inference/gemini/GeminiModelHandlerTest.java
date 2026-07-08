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
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.ml.inference.remote.PredictionResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GeminiModelHandlerTest {

  @Test
  public void testAllParamsSet() {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setApiKey("test-key")
            .setProject("test-project")
            .setLocation("us-central1")
            .setModelName("gemini-model-123")
            .setRequestFn(GeminiInferenceFunctions.generateFromString())
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    assertThrows(IllegalArgumentException.class, () -> handler.createClient(parameters));
  }

  @Test
  public void testMissingVertexLocationParam() {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setProject("test-project")
            .setModelName("gemini-model-123")
            .setRequestFn(GeminiInferenceFunctions.generateFromString())
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    assertThrows(IllegalArgumentException.class, () -> handler.createClient(parameters));
  }

  @Test
  public void testMissingVertexProjectParam() {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setLocation("us-central1")
            .setModelName("gemini-model-123")
            .setRequestFn(GeminiInferenceFunctions.generateFromString())
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    assertThrows(IllegalArgumentException.class, () -> handler.createClient(parameters));
  }

  @Test
  public void testMissingAllParams() {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setModelName("gemini-model-123")
            .setRequestFn(GeminiInferenceFunctions.generateFromString())
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    assertThrows(IllegalArgumentException.class, () -> handler.createClient(parameters));
  }

  @Test
  public void testRequest() throws Exception {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setApiKey("test-key")
            .setModelName("gemini-model-123")
            .setRequestFn((modelName, batch, client) -> Arrays.asList("response1", "response2"))
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    handler.createClient(parameters);

    List<String> input = Arrays.asList("input1", "input2");
    Iterable<PredictionResult<String, String>> results = handler.request(input);

    int count = 0;
    for (PredictionResult<String, String> result : results) {
      if (count == 0) {
        assertEquals("input1", result.getInput());
        assertEquals("response1", result.getOutput());
      } else {
        assertEquals("input2", result.getInput());
        assertEquals("response2", result.getOutput());
      }
      count++;
    }
    assertEquals(2, count);
  }

  @Test
  public void testRequestMismatchedResponseSize() throws Exception {
    GeminiModelParameters<String, String> parameters =
        GeminiModelParameters.<String, String>builder()
            .setApiKey("test-key")
            .setModelName("gemini-model-123")
            .setRequestFn((modelName, batch, client) -> Arrays.asList("response1"))
            .build();
    GeminiModelHandler<String, String> handler = new GeminiModelHandler<>();
    handler.createClient(parameters);

    List<String> input = Arrays.asList("input1", "input2");
    assertThrows(RuntimeException.class, () -> handler.request(input));
  }
}
