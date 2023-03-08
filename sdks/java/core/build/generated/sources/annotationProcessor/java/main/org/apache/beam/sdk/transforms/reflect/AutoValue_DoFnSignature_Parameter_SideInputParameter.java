package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_SideInputParameter extends DoFnSignature.Parameter.SideInputParameter {

  private final TypeDescriptor<?> elementT;

  private final String sideInputId;

  private AutoValue_DoFnSignature_Parameter_SideInputParameter(
      TypeDescriptor<?> elementT,
      String sideInputId) {
    this.elementT = elementT;
    this.sideInputId = sideInputId;
  }

  @Override
  public TypeDescriptor<?> elementT() {
    return elementT;
  }

  @Override
  public String sideInputId() {
    return sideInputId;
  }

  @Override
  public String toString() {
    return "SideInputParameter{"
        + "elementT=" + elementT + ", "
        + "sideInputId=" + sideInputId
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.SideInputParameter) {
      DoFnSignature.Parameter.SideInputParameter that = (DoFnSignature.Parameter.SideInputParameter) o;
      return this.elementT.equals(that.elementT())
          && this.sideInputId.equals(that.sideInputId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= elementT.hashCode();
    h$ *= 1000003;
    h$ ^= sideInputId.hashCode();
    return h$;
  }

  @Override
  public DoFnSignature.Parameter.SideInputParameter.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends DoFnSignature.Parameter.SideInputParameter.Builder {
    private TypeDescriptor<?> elementT;
    private String sideInputId;
    Builder() {
    }
    private Builder(DoFnSignature.Parameter.SideInputParameter source) {
      this.elementT = source.elementT();
      this.sideInputId = source.sideInputId();
    }
    @Override
    public DoFnSignature.Parameter.SideInputParameter.Builder setElementT(TypeDescriptor<?> elementT) {
      if (elementT == null) {
        throw new NullPointerException("Null elementT");
      }
      this.elementT = elementT;
      return this;
    }
    @Override
    public DoFnSignature.Parameter.SideInputParameter.Builder setSideInputId(String sideInputId) {
      if (sideInputId == null) {
        throw new NullPointerException("Null sideInputId");
      }
      this.sideInputId = sideInputId;
      return this;
    }
    @Override
    public DoFnSignature.Parameter.SideInputParameter build() {
      if (this.elementT == null
          || this.sideInputId == null) {
        StringBuilder missing = new StringBuilder();
        if (this.elementT == null) {
          missing.append(" elementT");
        }
        if (this.sideInputId == null) {
          missing.append(" sideInputId");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_DoFnSignature_Parameter_SideInputParameter(
          this.elementT,
          this.sideInputId);
    }
  }

}
