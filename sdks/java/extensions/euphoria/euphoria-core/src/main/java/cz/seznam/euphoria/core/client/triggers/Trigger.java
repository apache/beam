
package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;

import java.io.Serializable;

/**
 * Trigger determines when a window result should be flushed.
 */
public interface Trigger extends Serializable {

  /**
   * Gives trigger a chance to schedule time-based events in provided context
   * @return {@code NOOP} or {@code PASSED} if desired trigger time passed
   */
  TriggerResult schedule(WindowContext w, TriggerContext ctx);

  /**
   * Called when a timer that was set using the trigger context fires.
   *
   * @param time The timestamp for which the timer was registered.
   * @param ctx A context object that can be used to register timer callbacks.
   */
  TriggerResult onTimeEvent(long time, WindowContext w, TriggerContext ctx);

  /**
   * Represents result returned from scheduling methods.
   */
  enum TriggerResult {

    /**
     * No action is taken on the window.
     */
    NOOP(false, false),

    /**
     * {@code FLUSH_AND_PURGE} evaluates the window function and emits the window
     * result.
     */
    FLUSH_AND_PURGE(true, true),

    /**
     * On {@code FLUSH}, the window is evaluated and results are emitted.
     * The window is not purged, though, the internal state is retained.
     */
    FLUSH(true, false),

    /**
     * All elements in the window are cleared and the window is discarded,
     * without evaluating the window function or emitting any elements.
     */
    PURGE(false, true),

    /**
     * The actual time this trigger should have triggered already passed
     */
    PASSED(false, false);

    // ------------------------------------------------------------------------

    private final boolean fire;
    private final boolean purge;

    TriggerResult(boolean flush, boolean purge) {
      this.purge = purge;
      this.fire = flush;
    }

    public boolean isFlush() {
      return fire;
    }

    public boolean isPurge() {
      return purge;
    }
  }
}
