package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.core.ApiClock;
import com.google.api.gax.rpc.Watchdog;
import com.google.api.gax.rpc.WatchdogProvider;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.threeten.bp.Duration;
import com.google.common.base.Preconditions;

/**
 * This class is copied from the {@link com.google.api.gax.rpc.InstantiatingWatchdogProvider},
 * however it says to not auto close, so that the {@link org.apache.beam.sdk.util.UnboundedScheduledExecutorService}
 * we pass it doesn't try to get closed as well.
 */
public class NonClosingWatchdogProvider implements WatchdogProvider {
  @Nullable
  private final ApiClock clock;
  @Nullable private final ScheduledExecutorService executor;
  @Nullable private final Duration checkInterval;

  public static WatchdogProvider create() {
    return new NonClosingWatchdogProvider(null, null, null);
  }

  private NonClosingWatchdogProvider(
      @Nullable ApiClock clock,
      @Nullable ScheduledExecutorService executor,
      @Nullable Duration checkInterval) {
    this.clock = clock;
    this.executor = executor;
    this.checkInterval = checkInterval;
  }

  @Override
  public boolean needsClock() {
    return clock == null;
  }

  @Override
  public WatchdogProvider withClock(@Nonnull ApiClock clock) {
    return new NonClosingWatchdogProvider(
        Preconditions.checkNotNull(clock), executor, checkInterval);
  }

  @Override
  public boolean needsCheckInterval() {
    return checkInterval == null;
  }

  @Override
  public WatchdogProvider withCheckInterval(@Nonnull Duration checkInterval) {
    return new NonClosingWatchdogProvider(
        clock, executor, Preconditions.checkNotNull(checkInterval));
  }

  @Override
  public boolean needsExecutor() {
    return executor == null;
  }

  @Override
  public WatchdogProvider withExecutor(ScheduledExecutorService executor) {
    return new NonClosingWatchdogProvider(
        clock, Preconditions.checkNotNull(executor), checkInterval);
  }

  @SuppressWarnings("ConstantConditions")
  @Nullable
  @Override
  public Watchdog getWatchdog() {
    Preconditions.checkState(!needsClock(), "A clock is needed");
    Preconditions.checkState(!needsCheckInterval(), "A check interval is needed");
    Preconditions.checkState(!needsExecutor(), "An executor is needed");

    // Watchdog is disabled
    if (checkInterval.isZero()) {
      return null;
    }

    return Watchdog.create(clock, checkInterval, executor);
  }

  @Override
  public boolean shouldAutoClose() {
    return false;
  }

}