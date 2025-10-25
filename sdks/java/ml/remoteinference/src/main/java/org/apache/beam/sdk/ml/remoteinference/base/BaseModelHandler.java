package org.apache.beam.sdk.ml.remoteinference.base;

public interface BaseModelHandler<ParamT extends BaseModelParameters, InputT extends BaseInput, OutputT extends BaseResponse> {

  // initialize the model with provided parameters
  public void createClient(ParamT parameters);

  // Logic to invoke model provider
  public OutputT request(InputT input);

}
