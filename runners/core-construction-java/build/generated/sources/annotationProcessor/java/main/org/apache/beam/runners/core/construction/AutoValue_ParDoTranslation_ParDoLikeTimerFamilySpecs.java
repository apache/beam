package org.apache.beam.runners.core.construction;

import java.util.Map;
import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ParDoTranslation_ParDoLikeTimerFamilySpecs extends ParDoTranslation.ParDoLikeTimerFamilySpecs {

  private final Map<String, RunnerApi.TimerFamilySpec> timerFamilySpecs;

  private final String onWindowExpirationTimerFamilySpec;

  AutoValue_ParDoTranslation_ParDoLikeTimerFamilySpecs(
      Map<String, RunnerApi.TimerFamilySpec> timerFamilySpecs,
      @Nullable String onWindowExpirationTimerFamilySpec) {
    if (timerFamilySpecs == null) {
      throw new NullPointerException("Null timerFamilySpecs");
    }
    this.timerFamilySpecs = timerFamilySpecs;
    this.onWindowExpirationTimerFamilySpec = onWindowExpirationTimerFamilySpec;
  }

  @Override
  Map<String, RunnerApi.TimerFamilySpec> timerFamilySpecs() {
    return timerFamilySpecs;
  }

  @Nullable
  @Override
  String onWindowExpirationTimerFamilySpec() {
    return onWindowExpirationTimerFamilySpec;
  }

  @Override
  public String toString() {
    return "ParDoLikeTimerFamilySpecs{"
        + "timerFamilySpecs=" + timerFamilySpecs + ", "
        + "onWindowExpirationTimerFamilySpec=" + onWindowExpirationTimerFamilySpec
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ParDoTranslation.ParDoLikeTimerFamilySpecs) {
      ParDoTranslation.ParDoLikeTimerFamilySpecs that = (ParDoTranslation.ParDoLikeTimerFamilySpecs) o;
      return this.timerFamilySpecs.equals(that.timerFamilySpecs())
          && (this.onWindowExpirationTimerFamilySpec == null ? that.onWindowExpirationTimerFamilySpec() == null : this.onWindowExpirationTimerFamilySpec.equals(that.onWindowExpirationTimerFamilySpec()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= timerFamilySpecs.hashCode();
    h$ *= 1000003;
    h$ ^= (onWindowExpirationTimerFamilySpec == null) ? 0 : onWindowExpirationTimerFamilySpec.hashCode();
    return h$;
  }

}
