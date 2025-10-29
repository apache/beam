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

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.ResponseCreateParams;
import org.apache.beam.sdk.ml.remoteinference.base.BaseModelHandler;
import org.apache.beam.sdk.ml.remoteinference.base.PredictionResult;

import java.util.List;
import java.util.stream.Collectors;

public class OpenAIModelHandler
  implements BaseModelHandler<OpenAIModelParameters, OpenAIModelInput, OpenAIModelResponse> {

  private transient OpenAIClient client;
  private transient ResponseCreateParams clientParams;
  private OpenAIModelParameters modelParameters;

  @Override
  public void createClient(OpenAIModelParameters parameters) {
    this.modelParameters = parameters;
    this.client = OpenAIOkHttpClient.builder()
      .apiKey(this.modelParameters.getApiKey())
      .build();
  }

  @Override
  public Iterable<PredictionResult<OpenAIModelInput, OpenAIModelResponse>> request(OpenAIModelInput input) {

    this.clientParams = ResponseCreateParams.builder()
      .model(this.modelParameters.getModelName())
      .input(input.getInput())
      .build();

    String output = client.responses().create(clientParams).output().stream()
      .flatMap(item -> item.message().stream())
      .flatMap(message -> message.content().stream())
      .flatMap(content -> content.outputText().stream())
      .map(outputText -> outputText.text())
      .collect(Collectors.joining());

    return List.of(PredictionResult.create(input, OpenAIModelResponse.create(output)));
  }

}
