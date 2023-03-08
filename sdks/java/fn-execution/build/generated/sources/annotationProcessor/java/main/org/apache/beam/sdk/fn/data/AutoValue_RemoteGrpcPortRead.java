package org.apache.beam.sdk.fn.data;

import javax.annotation.Generated;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RemoteGrpcPortRead extends RemoteGrpcPortRead {

  private final BeamFnApi.RemoteGrpcPort port;

  private final String outputPCollectionId;

  AutoValue_RemoteGrpcPortRead(
      BeamFnApi.RemoteGrpcPort port,
      String outputPCollectionId) {
    if (port == null) {
      throw new NullPointerException("Null port");
    }
    this.port = port;
    if (outputPCollectionId == null) {
      throw new NullPointerException("Null outputPCollectionId");
    }
    this.outputPCollectionId = outputPCollectionId;
  }

  @Override
  public BeamFnApi.RemoteGrpcPort getPort() {
    return port;
  }

  @Override
  String getOutputPCollectionId() {
    return outputPCollectionId;
  }

  @Override
  public String toString() {
    return "RemoteGrpcPortRead{"
        + "port=" + port + ", "
        + "outputPCollectionId=" + outputPCollectionId
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RemoteGrpcPortRead) {
      RemoteGrpcPortRead that = (RemoteGrpcPortRead) o;
      return this.port.equals(that.getPort())
          && this.outputPCollectionId.equals(that.getOutputPCollectionId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= port.hashCode();
    h$ *= 1000003;
    h$ ^= outputPCollectionId.hashCode();
    return h$;
  }

}
