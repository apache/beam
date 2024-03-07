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
package org.apache.beam.examples.webapis;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.SetupTeardown;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

// [START webapis_java_gemini_ai_client]

/**
 * Example {@link Caller} and {@link SetupTeardown} implementation for use with {@link
 * RequestResponseIO} to process Gemini AI {@link GenerateContentRequest}s into {@link
 * GenerateContentResponse}s.
 */
@AutoValue
abstract class GeminiAIClient
    implements Caller<KV<String, GenerateContentRequest>, KV<String, GenerateContentResponse>>,
        SetupTeardown {

  static Builder builder() {
    return new AutoValue_GeminiAIClient.Builder();
  }

  static final String MODEL_GEMINI_PRO = "gemini-pro";
  static final String MODEL_GEMINI_PRO_VISION = "gemini-pro-vision";

  private transient @MonotonicNonNull VertexAI vertexAI;
  private transient @MonotonicNonNull GenerativeModel client;

  @Override
  public KV<String, GenerateContentResponse> call(KV<String, GenerateContentRequest> requestKV)
      throws UserCodeExecutionException {

    String key = requestKV.getKey();
    GenerateContentRequest request = requestKV.getValue();

    if (request == null) {
      throw new UserCodeExecutionException("request is empty");
    }

    if (request.getContentsList().isEmpty()) {
      throw new UserCodeExecutionException("contentsList is empty");
    }

    try {

      GenerateContentResponse response =
          checkStateNotNull(client).generateContent(request.getContentsList());

      return KV.of(key, response);

    } catch (IOException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  @Override
  public void setup() throws UserCodeExecutionException {
    vertexAI = new VertexAI(getProjectId(), getLocation());
    client = new GenerativeModel(getModelName(), vertexAI);
  }

  @Override
  public void teardown() throws UserCodeExecutionException {
    if (vertexAI != null) {
      vertexAI.close();
    }
  }

  abstract String getModelName();

  abstract String getProjectId();

  abstract String getLocation();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setModelName(String name);

    abstract Optional<String> getModelName();

    abstract Builder setProjectId(String value);

    abstract Builder setLocation(String value);

    abstract GeminiAIClient autoBuild();

    final GeminiAIClient build() {
      if (!getModelName().isPresent()) {
        setModelName(MODEL_GEMINI_PRO);
      }
      return autoBuild();
    }
  }

  // [END webapis_java_gemini_ai_client]
}
