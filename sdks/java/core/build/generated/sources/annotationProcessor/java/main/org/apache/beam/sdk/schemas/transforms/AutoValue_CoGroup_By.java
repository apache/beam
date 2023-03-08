package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_CoGroup_By extends CoGroup.By {

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private final boolean optionalParticipation;

  private final boolean sideInput;

  private AutoValue_CoGroup_By(
      FieldAccessDescriptor fieldAccessDescriptor,
      boolean optionalParticipation,
      boolean sideInput) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
    this.optionalParticipation = optionalParticipation;
    this.sideInput = sideInput;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  boolean getOptionalParticipation() {
    return optionalParticipation;
  }

  @Override
  boolean getSideInput() {
    return sideInput;
  }

  @Override
  public String toString() {
    return "By{"
        + "fieldAccessDescriptor=" + fieldAccessDescriptor + ", "
        + "optionalParticipation=" + optionalParticipation + ", "
        + "sideInput=" + sideInput
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CoGroup.By) {
      CoGroup.By that = (CoGroup.By) o;
      return this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor())
          && this.optionalParticipation == that.getOptionalParticipation()
          && this.sideInput == that.getSideInput();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= optionalParticipation ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= sideInput ? 1231 : 1237;
    return h$;
  }

  @Override
  CoGroup.By.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends CoGroup.By.Builder {
    private FieldAccessDescriptor fieldAccessDescriptor;
    private Boolean optionalParticipation;
    private Boolean sideInput;
    Builder() {
    }
    private Builder(CoGroup.By source) {
      this.fieldAccessDescriptor = source.getFieldAccessDescriptor();
      this.optionalParticipation = source.getOptionalParticipation();
      this.sideInput = source.getSideInput();
    }
    @Override
    CoGroup.By.Builder setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      if (fieldAccessDescriptor == null) {
        throw new NullPointerException("Null fieldAccessDescriptor");
      }
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      return this;
    }
    @Override
    CoGroup.By.Builder setOptionalParticipation(boolean optionalParticipation) {
      this.optionalParticipation = optionalParticipation;
      return this;
    }
    @Override
    CoGroup.By.Builder setSideInput(boolean sideInput) {
      this.sideInput = sideInput;
      return this;
    }
    @Override
    CoGroup.By build() {
      if (this.fieldAccessDescriptor == null
          || this.optionalParticipation == null
          || this.sideInput == null) {
        StringBuilder missing = new StringBuilder();
        if (this.fieldAccessDescriptor == null) {
          missing.append(" fieldAccessDescriptor");
        }
        if (this.optionalParticipation == null) {
          missing.append(" optionalParticipation");
        }
        if (this.sideInput == null) {
          missing.append(" sideInput");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_CoGroup_By(
          this.fieldAccessDescriptor,
          this.optionalParticipation,
          this.sideInput);
    }
  }

}
