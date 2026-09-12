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

import com.google.genai.Client;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.ml.inference.remote.BaseInput;
import org.apache.beam.sdk.ml.inference.remote.BaseModelHandler;
import org.apache.beam.sdk.ml.inference.remote.BaseResponse;
import org.apache.beam.sdk.ml.inference.remote.PredictionResult;

/**
 * Model handler for Google Gemini API inference requests.
 *
 * <p>This handler manages communication with Google's Gemini API, including client initialization,
 * request formatting, and response parsing. It allows executing a custom {@link
 * GeminiRequestFunction} against a batch of inputs.
 */
@SuppressWarnings("nullness")
public class GeminiModelHandler<InputT extends BaseInput, OutputT extends BaseResponse>
    implements BaseModelHandler<GeminiModelParameters<InputT, OutputT>, InputT, OutputT> {

  private transient Client client;
  private GeminiModelParameters<InputT, OutputT> modelParameters;

  @Override
  public void createClient(GeminiModelParameters<InputT, OutputT> parameters) {
    if (parameters == null) {
      throw new NullPointerException("GeminiModelParameters must not be null");
    }
    this.modelParameters = parameters;

    // Configure client based on vertex or API key
    if (parameters.getApiKey() != null) {
      if (parameters.getProject() != null || parameters.getLocation() != null) {
        throw new IllegalArgumentException("Project and location must be null if API key is set");
      }
      this.client = Client.builder().apiKey(parameters.getApiKey()).build();
    } else {
      Client.Builder builder = Client.builder();
      if (parameters.getProject() != null && parameters.getLocation() != null) {
        builder.vertexAI(true).project(parameters.getProject()).location(parameters.getLocation());
      } else if (parameters.getProject() != null || parameters.getLocation() != null) {
        throw new IllegalArgumentException(
            "Project and location must both be provided if one is provided");
      }
      this.client = builder.build();
    }
  }

  @Override
  public Iterable<PredictionResult<InputT, OutputT>> request(List<InputT> input) {
    try {
      GeminiRequestFunction<InputT, OutputT> requestFn = modelParameters.getRequestFn();
      List<OutputT> responses = requestFn.apply(modelParameters.getModelName(), input, client);

      if (responses.size() != input.size()) {
        throw new IllegalStateException("Number of responses must match number of inputs");
      }

      List<PredictionResult<InputT, OutputT>> results = new ArrayList<>();
      for (int i = 0; i < input.size(); i++) {
        results.add(PredictionResult.create(input.get(i), responses.get(i)));
      }
      return results;
    } catch (Exception e) {
      throw new RuntimeException("Error during Gemini inference request", e);
    }
  }
}
