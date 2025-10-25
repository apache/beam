package org.apache.beam.sdk.ml.remoteinference.openai;

import org.apache.beam.sdk.ml.remoteinference.base.BaseInput;

public class OpenAiModelInput extends BaseInput {

  private final String input;

  private OpenAiModelInput(String input) {

    this.input = input;
  }

  public String getInput() {
    return input;
  }

  public static OpenAiModelInput create(String input) {
    return new OpenAiModelInput(input);
  }

}
