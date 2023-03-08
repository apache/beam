package org.apache.beam.sdk.fn.data;

import javax.annotation.Generated;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RemoteGrpcPortWrite extends RemoteGrpcPortWrite {

  private final String inputPCollectionId;

  private final BeamFnApi.RemoteGrpcPort port;

  AutoValue_RemoteGrpcPortWrite(
      String inputPCollectionId,
      BeamFnApi.RemoteGrpcPort port) {
    if (inputPCollectionId == null) {
      throw new NullPointerException("Null inputPCollectionId");
    }
    this.inputPCollectionId = inputPCollectionId;
    if (port == null) {
      throw new NullPointerException("Null port");
    }
    this.port = port;
  }

  @Override
  String getInputPCollectionId() {
    return inputPCollectionId;
  }

  @Override
  public BeamFnApi.RemoteGrpcPort getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "RemoteGrpcPortWrite{"
        + "inputPCollectionId=" + inputPCollectionId + ", "
        + "port=" + port
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RemoteGrpcPortWrite) {
      RemoteGrpcPortWrite that = (RemoteGrpcPortWrite) o;
      return this.inputPCollectionId.equals(that.getInputPCollectionId())
          && this.port.equals(that.getPort());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= inputPCollectionId.hashCode();
    h$ *= 1000003;
    h$ ^= port.hashCode();
    return h$;
  }

}
