package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_OutputReceiverParameter extends DoFnSignature.Parameter.OutputReceiverParameter {

  private final boolean rowReceiver;

  AutoValue_DoFnSignature_Parameter_OutputReceiverParameter(
      boolean rowReceiver) {
    this.rowReceiver = rowReceiver;
  }

  @Override
  public boolean isRowReceiver() {
    return rowReceiver;
  }

  @Override
  public String toString() {
    return "OutputReceiverParameter{"
        + "rowReceiver=" + rowReceiver
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.OutputReceiverParameter) {
      DoFnSignature.Parameter.OutputReceiverParameter that = (DoFnSignature.Parameter.OutputReceiverParameter) o;
      return this.rowReceiver == that.isRowReceiver();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= rowReceiver ? 1231 : 1237;
    return h$;
  }

}
