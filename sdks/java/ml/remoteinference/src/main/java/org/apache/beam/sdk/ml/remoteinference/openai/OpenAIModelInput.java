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

import org.apache.beam.sdk.ml.remoteinference.base.BaseInput;

/**
 * Input for OpenAI model inference requests.
 *
 * <p>This class encapsulates text input to be sent to OpenAI models.
 *
 * <h3>Example Usage</h3>
 * <pre>{@code
 * OpenAIModelInput input = OpenAIModelInput.create("Translate to French: Hello");
 * String text = input.getModelInput(); // "Translate to French: Hello"
 * }</pre>
 *
 * @see OpenAIModelHandler
 * @see OpenAIModelResponse
 */
public class OpenAIModelInput extends BaseInput {

  private final String input;

  private OpenAIModelInput(String input) {

    this.input = input;
  }

  /**
   * Returns the text input for the model.
   *
   * @return the input text string
   */
  public String getModelInput() {
    return input;
  }

  /**
   * Creates a new input instance with the specified text.
   *
   * @param input the text to send to the model
   * @return a new {@link OpenAIModelInput} instance
   */
  public static OpenAIModelInput create(String input) {
    return new OpenAIModelInput(input);
  }

}
