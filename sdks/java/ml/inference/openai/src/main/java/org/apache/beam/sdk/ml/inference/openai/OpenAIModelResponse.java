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

import org.apache.beam.sdk.ml.inference.remote.BaseResponse;

/**
 * Response from OpenAI model inference results.
 * <p>This class encapsulates the text output returned from OpenAI models..
 *
 * <h3>Example Usage</h3>
 * <pre>{@code
 * OpenAIModelResponse response = OpenAIModelResponse.create("Bonjour");
 * String output = response.getModelResponse(); // "Bonjour"
 * }</pre>
 *
 * @see OpenAIModelHandler
 * @see OpenAIModelInput
 */
public class OpenAIModelResponse implements BaseResponse {

  private final String output;

  private OpenAIModelResponse(String output) {
    this.output = output;
  }

  /**
   * Returns the text output from the model.
   *
   * @return the output text string
   */
  public String getModelResponse() {
    return output;
  }

  /**
   * Creates a new response instance with the specified output text.
   *
   * @param output the text returned by the model
   * @return a new {@link OpenAIModelResponse} instance
   */
  public static OpenAIModelResponse create(String output) {
    return new OpenAIModelResponse(output);
  }
}
