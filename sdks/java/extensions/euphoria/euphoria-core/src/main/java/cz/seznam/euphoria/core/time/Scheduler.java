package cz.seznam.euphoria.core.time;

import java.time.Duration;

public interface Scheduler {

  /**
   * Submit the given runnable for repeated execution.
   */
  void schedulePeriodically(Duration period, Runnable r);

  /**
   * Cancel all scheduled tasks
   */
  void shutdown();

}
