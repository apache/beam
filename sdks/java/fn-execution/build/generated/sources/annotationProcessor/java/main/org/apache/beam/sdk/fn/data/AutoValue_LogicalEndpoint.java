package org.apache.beam.sdk.fn.data;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_LogicalEndpoint extends LogicalEndpoint {

  private final String instructionId;

  private final String transformId;

  private final @Nullable String timerFamilyId;

  AutoValue_LogicalEndpoint(
      String instructionId,
      String transformId,
      @Nullable String timerFamilyId) {
    if (instructionId == null) {
      throw new NullPointerException("Null instructionId");
    }
    this.instructionId = instructionId;
    if (transformId == null) {
      throw new NullPointerException("Null transformId");
    }
    this.transformId = transformId;
    this.timerFamilyId = timerFamilyId;
  }

  @Override
  public String getInstructionId() {
    return instructionId;
  }

  @Override
  public String getTransformId() {
    return transformId;
  }

  @Override
  public @Nullable String getTimerFamilyId() {
    return timerFamilyId;
  }

  @Override
  public String toString() {
    return "LogicalEndpoint{"
        + "instructionId=" + instructionId + ", "
        + "transformId=" + transformId + ", "
        + "timerFamilyId=" + timerFamilyId
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof LogicalEndpoint) {
      LogicalEndpoint that = (LogicalEndpoint) o;
      return this.instructionId.equals(that.getInstructionId())
          && this.transformId.equals(that.getTransformId())
          && (this.timerFamilyId == null ? that.getTimerFamilyId() == null : this.timerFamilyId.equals(that.getTimerFamilyId()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= instructionId.hashCode();
    h$ *= 1000003;
    h$ ^= transformId.hashCode();
    h$ *= 1000003;
    h$ ^= (timerFamilyId == null) ? 0 : timerFamilyId.hashCode();
    return h$;
  }

}
