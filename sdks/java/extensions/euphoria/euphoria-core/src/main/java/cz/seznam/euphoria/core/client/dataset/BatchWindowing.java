
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class BatchWindowing<T>
    extends AbstractWindowing<T, Void, BatchWindowing.BatchWindow>
    implements AlignedWindowing<T, BatchWindowing.BatchWindow> {


  public static class BatchWindow
      extends AbstractWindow<Void, BatchWindow>
      implements AlignedWindow<BatchWindow> {


    @Override
    public void registerTrigger(Triggering triggering,
        UnaryFunction<BatchWindow, Void> evict) {
      // the batch window will end at the end of input stream
    }

  }

  // batch windowing singleton
  final static BatchWindowing<?> singleton = new BatchWindowing<>();
  // window singleton
  final BatchWindow window = new BatchWindow();

  private BatchWindowing() {
    this.addNewWindow(window, null, null);
  }

  @Override
  public Set<BatchWindow> allocateWindows(T what, Triggering triggering,
      UnaryFunction<BatchWindow, Void> evict) {
    // batch windowing has only single window
    return getActive(null);
  }

  @SuppressWarnings("unchecked")
  public static <T> BatchWindowing<T> get() {
    return (BatchWindowing) singleton;
  }

}
