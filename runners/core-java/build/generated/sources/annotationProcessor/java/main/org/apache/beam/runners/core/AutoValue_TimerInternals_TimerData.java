package org.apache.beam.runners.core;

import javax.annotation.Generated;
import org.apache.beam.sdk.state.TimeDomain;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimerInternals_TimerData extends TimerInternals.TimerData {

  private final String timerId;

  private final String timerFamilyId;

  private final StateNamespace namespace;

  private final Instant timestamp;

  private final Instant outputTimestamp;

  private final TimeDomain domain;

  private final boolean deleted;

  AutoValue_TimerInternals_TimerData(
      String timerId,
      String timerFamilyId,
      StateNamespace namespace,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain domain,
      boolean deleted) {
    if (timerId == null) {
      throw new NullPointerException("Null timerId");
    }
    this.timerId = timerId;
    if (timerFamilyId == null) {
      throw new NullPointerException("Null timerFamilyId");
    }
    this.timerFamilyId = timerFamilyId;
    if (namespace == null) {
      throw new NullPointerException("Null namespace");
    }
    this.namespace = namespace;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
    if (outputTimestamp == null) {
      throw new NullPointerException("Null outputTimestamp");
    }
    this.outputTimestamp = outputTimestamp;
    if (domain == null) {
      throw new NullPointerException("Null domain");
    }
    this.domain = domain;
    this.deleted = deleted;
  }

  @Override
  public String getTimerId() {
    return timerId;
  }

  @Override
  public String getTimerFamilyId() {
    return timerFamilyId;
  }

  @Override
  public StateNamespace getNamespace() {
    return namespace;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public Instant getOutputTimestamp() {
    return outputTimestamp;
  }

  @Override
  public TimeDomain getDomain() {
    return domain;
  }

  @Override
  public boolean getDeleted() {
    return deleted;
  }

  @Override
  public String toString() {
    return "TimerData{"
        + "timerId=" + timerId + ", "
        + "timerFamilyId=" + timerFamilyId + ", "
        + "namespace=" + namespace + ", "
        + "timestamp=" + timestamp + ", "
        + "outputTimestamp=" + outputTimestamp + ", "
        + "domain=" + domain + ", "
        + "deleted=" + deleted
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimerInternals.TimerData) {
      TimerInternals.TimerData that = (TimerInternals.TimerData) o;
      return this.timerId.equals(that.getTimerId())
          && this.timerFamilyId.equals(that.getTimerFamilyId())
          && this.namespace.equals(that.getNamespace())
          && this.timestamp.equals(that.getTimestamp())
          && this.outputTimestamp.equals(that.getOutputTimestamp())
          && this.domain.equals(that.getDomain())
          && this.deleted == that.getDeleted();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= timerId.hashCode();
    h$ *= 1000003;
    h$ ^= timerFamilyId.hashCode();
    h$ *= 1000003;
    h$ ^= namespace.hashCode();
    h$ *= 1000003;
    h$ ^= timestamp.hashCode();
    h$ *= 1000003;
    h$ ^= outputTimestamp.hashCode();
    h$ *= 1000003;
    h$ ^= domain.hashCode();
    h$ *= 1000003;
    h$ ^= deleted ? 1231 : 1237;
    return h$;
  }

}
