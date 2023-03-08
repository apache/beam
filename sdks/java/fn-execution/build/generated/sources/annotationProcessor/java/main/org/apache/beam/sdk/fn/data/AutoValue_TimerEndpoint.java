package org.apache.beam.sdk.fn.data;

import javax.annotation.Generated;
import org.apache.beam.sdk.coders.Coder;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimerEndpoint<T> extends TimerEndpoint<T> {

  private final String transformId;

  private final String timerFamilyId;

  private final Coder<T> coder;

  private final FnDataReceiver<T> receiver;

  AutoValue_TimerEndpoint(
      String transformId,
      String timerFamilyId,
      Coder<T> coder,
      FnDataReceiver<T> receiver) {
    if (transformId == null) {
      throw new NullPointerException("Null transformId");
    }
    this.transformId = transformId;
    if (timerFamilyId == null) {
      throw new NullPointerException("Null timerFamilyId");
    }
    this.timerFamilyId = timerFamilyId;
    if (coder == null) {
      throw new NullPointerException("Null coder");
    }
    this.coder = coder;
    if (receiver == null) {
      throw new NullPointerException("Null receiver");
    }
    this.receiver = receiver;
  }

  @Override
  public String getTransformId() {
    return transformId;
  }

  @Override
  public String getTimerFamilyId() {
    return timerFamilyId;
  }

  @Override
  public Coder<T> getCoder() {
    return coder;
  }

  @Override
  public FnDataReceiver<T> getReceiver() {
    return receiver;
  }

  @Override
  public String toString() {
    return "TimerEndpoint{"
        + "transformId=" + transformId + ", "
        + "timerFamilyId=" + timerFamilyId + ", "
        + "coder=" + coder + ", "
        + "receiver=" + receiver
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimerEndpoint) {
      TimerEndpoint<?> that = (TimerEndpoint<?>) o;
      return this.transformId.equals(that.getTransformId())
          && this.timerFamilyId.equals(that.getTimerFamilyId())
          && this.coder.equals(that.getCoder())
          && this.receiver.equals(that.getReceiver());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= transformId.hashCode();
    h$ *= 1000003;
    h$ ^= timerFamilyId.hashCode();
    h$ *= 1000003;
    h$ ^= coder.hashCode();
    h$ *= 1000003;
    h$ ^= receiver.hashCode();
    return h$;
  }

}
