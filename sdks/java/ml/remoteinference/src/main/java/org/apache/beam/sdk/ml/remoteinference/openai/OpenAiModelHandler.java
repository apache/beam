package org.apache.beam.sdk.ml.remoteinference.openai;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.responses.ResponseCreateParams;
import org.apache.beam.sdk.ml.remoteinference.base.BaseModelHandler;

import java.util.stream.Collectors;

public class OpenAiModelHandler
  implements BaseModelHandler<OpenAiModelParameters, OpenAiModelInput, OpenAiModelResponse> {

  private transient OpenAIClient client;
  private transient ResponseCreateParams clientParams;
  private OpenAiModelParameters modelParameters;

  @Override
  public void createClient(OpenAiModelParameters parameters) {
    this.modelParameters = parameters;
    this.client = OpenAIOkHttpClient.builder()
      .apiKey(this.modelParameters.getApiKey())
      .build();
  }

  @Override
  public OpenAiModelResponse request(OpenAiModelInput input) {

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

    OpenAiModelResponse res = OpenAiModelResponse.create(input.getInput(), output);
    return res;
  }

}
