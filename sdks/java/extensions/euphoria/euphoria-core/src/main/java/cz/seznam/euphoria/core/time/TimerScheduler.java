package cz.seznam.euphoria.core.time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Override;import java.lang.Runnable;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A scheduler implementation with an underlying timer periodically executing
 * the specified runnables.
 */
public class TimerScheduler implements Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(TimerScheduler.class);

  final Timer timer = new Timer();

  @Override
  public void schedulePeriodically(final Duration period, final Runnable r) {
    final TimerTask t = new TimerTask() {
      @Override public void run() {
        LOG.debug("Running periodically scheduled {}", r);
        r.run();
      }
    };
    long millis = period.toMillis();
    timer.schedule(t, millis, millis);
  }

  @Override
  public void shutdown() {
    timer.cancel();
  }
}
