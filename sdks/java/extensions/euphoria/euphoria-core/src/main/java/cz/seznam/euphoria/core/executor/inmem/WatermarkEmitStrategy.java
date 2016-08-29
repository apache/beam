
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

    static final int COUNT_BEFORE_EMIT = 1;

    final static ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
    int itemsSeen = 0;

    @Override
    public void emitIfNeeded(Runnable action) {
      if (++itemsSeen > COUNT_BEFORE_EMIT) {
        itemsSeen = 0;
        action.run();
      }
    }

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
   * Emit watermark to given collector if needed.
   */
  void emitIfNeeded(Runnable action);


  /**
   * Schedule for periodic emitting.
   */
  void schedule(Runnable action);

  /**
   * Terminate the strategy. Used when gracefully shutting down the executor.
   */
  void close();

}
