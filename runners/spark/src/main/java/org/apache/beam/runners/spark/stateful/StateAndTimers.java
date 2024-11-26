package org.apache.beam.runners.spark.stateful;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

/** State and Timers wrapper. */
@AutoValue
public abstract class StateAndTimers implements Serializable {
  public abstract Table<String, String, byte[]> getState();

  public @Nullable abstract Collection<byte[]> getTimers();

  public static StateAndTimers of(
      final Table<String, String, byte[]> state, @Nullable final Collection<byte[]> timers) {
    return new AutoValue_StateAndTimers.Builder().setState(state).setTimers(timers).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setState(Table<String, String, byte[]> state);

    abstract Builder setTimers(Collection<byte[]> timers);

    abstract StateAndTimers build();
  }
}
