package org.apache.beam.sdk.ml.remoteinference.openai;

import org.apache.beam.sdk.ml.remoteinference.base.BaseResponse;

public class OpenAiModelResponse extends BaseResponse {

  private final String input;
  private final String output;

  private OpenAiModelResponse(String input, String output) {
    this.input = input;
    this.output = output;
  }

  public String getInput() {
    return input;
  }

  public String getOutput() {
    return output;
  }

  public static OpenAiModelResponse create(String input, String output) {
    return new OpenAiModelResponse(input, output);
  }
}
