/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.triggers;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

import java.io.Serializable;

/**
 * Trigger determines when a window result should be flushed.
 */
public interface Trigger<W extends Window> extends Serializable {

  /**
   * Determines whether this trigger implementation uses the trigger-context
   * to persist temporary state for windows.
   *
   * <p>Processing with stateless triggers can be optimized in certain situations.
   * Note that if this method returns {@code false} invocations to
   * {@link TriggerContext#getListStorage(ListStorageDescriptor)},
   * {@link TriggerContext#getValueStorage(ValueStorageDescriptor)},
   * or {@link TriggerContext.TriggerMergeContext#mergeStoredState(StorageDescriptorBase)}
   * may fail with an exception.
   *
   * <p>The default implementation always return {@code true}.
   */
  default boolean isStateful() {
    return true;
  }

  /**
   * Called for each element added to a window.
   *
   * @param time    Timestamp of the incoming element.
   * @param window  Window into which the element is being added.
   * @param ctx     Context instance that can be used to register timers.
   */
  TriggerResult onElement(long time, W window, TriggerContext ctx);

  /**
   * Called when a timer that was set using the trigger context fires.
   * <p>
   * In the case of a composite trigger (i.e. {@link AfterFirstCompositeTrigger})
   * a particular trigger might be invoked for a time which it has not registered.
   * Implementations are advised to validate the given {@code time}
   * and return {@code NOOP} in case the stamp
   * does not correspond to the particular trigger.
   *
   * @param time   The timestamp for which the timer was registered.
   * @param window Window that for which the time expired.
   * @param ctx    Context instance that can be used to register timers.
   */
  TriggerResult onTimer(long time, W window, TriggerContext ctx);

  /**
   * Called when the given window is purged. Trigger is given chance to perform
   * a final cleanup (e. g. un-register timers).
   *
   * @param window Window that is being purged.
   * @param ctx    Context that can be used to un-register timers.
   */
  void onClear(W window, TriggerContext ctx);

  /**
   * Called when multiple windows have been merged into one.
   *
   * @param window Resulting window from the merge operation.
   * @param ctx    Context instance
   */
  TriggerResult onMerge(W window, TriggerContext.TriggerMergeContext ctx);

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
    PURGE(false, true);

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

    /**
     * Merges two {@link TriggerResult}. This specifies what should happen if we have
     * two results from a {@link Trigger}.
     * <p>
     * For example, if one result says {@code NOOP} while the other says {@code FLUSH}
     * then {@code FLUSH} is the combined result;
     */
    public static TriggerResult merge(TriggerResult a, TriggerResult b) {
      if (a.purge || b.purge) {
        if (a.fire || b.fire) {
          return FLUSH_AND_PURGE;
        } else {
          return PURGE;
        }
      } else if (a.fire || b.fire) {
        return FLUSH;
      } else {
        return NOOP;
      }
    }
  }
}
