package org.apache.beam.sdk.state;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TimerSpecs_SimpleTimerSpec extends TimerSpecs.SimpleTimerSpec {

  private final TimeDomain timeDomain;

  AutoValue_TimerSpecs_SimpleTimerSpec(
      TimeDomain timeDomain) {
    if (timeDomain == null) {
      throw new NullPointerException("Null timeDomain");
    }
    this.timeDomain = timeDomain;
  }

  @Override
  public TimeDomain getTimeDomain() {
    return timeDomain;
  }

  @Override
  public String toString() {
    return "SimpleTimerSpec{"
        + "timeDomain=" + timeDomain
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TimerSpecs.SimpleTimerSpec) {
      TimerSpecs.SimpleTimerSpec that = (TimerSpecs.SimpleTimerSpec) o;
      return this.timeDomain.equals(that.getTimeDomain());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= timeDomain.hashCode();
    return h$;
  }

}
