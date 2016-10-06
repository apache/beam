
package cz.seznam.euphoria.core.executor.inmem;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Strategy for emitting watermarks.
 */
public interface WatermarkEmitStrategy {

  /** Default strategy used in inmem executor. */
  static class Default implements WatermarkEmitStrategy {

    final static ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);

    @Override
    public void schedule(Runnable action) {
      scheduler.scheduleAtFixedRate(action, 100, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
      scheduler.shutdown();
    }

  }

  /**
   * Schedule for periodic emitting.
   */
  void schedule(Runnable action);

  /**
   * Terminate the strategy. Used when gracefully shutting down the executor.
   */
  void close();

}
