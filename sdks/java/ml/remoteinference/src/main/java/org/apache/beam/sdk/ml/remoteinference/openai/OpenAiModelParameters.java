package org.apache.beam.sdk.ml.remoteinference.openai;

import org.apache.beam.sdk.ml.remoteinference.base.BaseModelParameters;

public class OpenAiModelParameters implements BaseModelParameters {

  private final String apiKey;
  private final String modelName;

  private OpenAiModelParameters(Builder builder) {
    this.apiKey = builder.apiKey;
    this.modelName = builder.modelName;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getModelName() {
    return modelName;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String apiKey;
    private String modelName;

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

    public OpenAiModelParameters build() {
      return new OpenAiModelParameters(this);
    }
  }
}
