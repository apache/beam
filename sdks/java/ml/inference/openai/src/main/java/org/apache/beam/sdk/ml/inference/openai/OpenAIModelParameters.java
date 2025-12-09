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

import org.apache.beam.sdk.ml.inference.remote.BaseModelParameters;

/**
 * Configuration parameters required for OpenAI model inference.
 *
 * <p>This class encapsulates all configuration needed to initialize and communicate with
 * OpenAI's API, including authentication credentials, model selection, and inference instructions.
 *
 * <h3>Example Usage</h3>
 * <pre>{@code
 * OpenAIModelParameters params = OpenAIModelParameters.builder()
 *     .apiKey("sk-...")
 *     .modelName("gpt-4")
 *     .instructionPrompt("Translate the following text to French:")
 *     .build();
 * }</pre>
 *
 * @see OpenAIModelHandler
 */
public class OpenAIModelParameters implements BaseModelParameters {

  private final String apiKey;
  private final String modelName;
  private final String instructionPrompt;

  private OpenAIModelParameters(Builder builder) {
    this.apiKey = builder.apiKey;
    this.modelName = builder.modelName;
    this.instructionPrompt = builder.instructionPrompt;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getModelName() {
    return modelName;
  }

  public String getInstructionPrompt() {
    return instructionPrompt;
  }

  public static Builder builder() {
    return new Builder();
  }


  public static class Builder {
    private String apiKey;
    private String modelName;
    private String instructionPrompt;

    private Builder() {
    }

    /**
     * Sets the OpenAI API key for authentication.
     *
     * @param apiKey the API key (required)
     */
    public Builder apiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    /**
     * Sets the name of the OpenAI model to use.
     *
     * @param modelName the model name, e.g., "gpt-4" (required)
     */
    public Builder modelName(String modelName) {
      this.modelName = modelName;
      return this;
    }
    /**
     * Sets the instruction prompt for the model.
     * This prompt provides context or instructions to the model about how to process
     * the input text.
     *
     * @param prompt the instruction text (required)
     */
    public Builder instructionPrompt(String prompt) {
      this.instructionPrompt = prompt;
      return this;
    }

    /**
     * Builds the {@link OpenAIModelParameters} instance.
     */
    public OpenAIModelParameters build() {
      return new OpenAIModelParameters(this);
    }
  }
}
