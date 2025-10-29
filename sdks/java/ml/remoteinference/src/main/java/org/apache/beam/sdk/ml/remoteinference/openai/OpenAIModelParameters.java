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

import org.apache.beam.sdk.ml.remoteinference.base.BaseModelParameters;

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

    public Builder apiKey(String apiKey) {
      this.apiKey = apiKey;
      return this;
    }

    public Builder modelName(String modelName) {
      this.modelName = modelName;
      return this;
    }

    public Builder instructionPrompt(String prompt) {
      this.instructionPrompt = prompt;
      return this;
    }

    public OpenAIModelParameters build() {
      return new OpenAIModelParameters(this);
    }
  }
}
