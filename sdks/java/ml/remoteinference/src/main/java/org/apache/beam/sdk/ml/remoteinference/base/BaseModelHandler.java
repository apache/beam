package org.apache.beam.sdk.ml.remoteinference.base;

public interface BaseModelHandler<ParamT extends BaseModelParameters> {

  // initialize the model client with provided parameters
  public void createClient(ParamT parameters);

  // Logic to invoke model provider
  public String request(String input);

}
