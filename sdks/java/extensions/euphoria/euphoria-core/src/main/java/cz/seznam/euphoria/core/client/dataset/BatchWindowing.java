
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Collections;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class BatchWindowing<T>
    extends AbstractWindowing<T, Void, BatchWindowing.BatchWindow>
    implements AlignedWindowing<T, BatchWindowing.BatchWindow>
{
  public static class BatchWindow
      extends AbstractWindow<Void>
      implements AlignedWindow {

    static final BatchWindow INSTANCE = new BatchWindow();

    private BatchWindow() {}

    @Override
    public void registerTrigger(Triggering triggering,
        UnaryFunction<Window<?>, Void> evict) {
      // the batch window will end at the end of input stream
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof BatchWindow;
    }

    @Override
    public int hashCode() {
      return 31;
    }
  } // ~ end of BatchWindow

  private final static BatchWindowing<?> INSTANCE = new BatchWindowing<>();
  private BatchWindowing() {}

  @Override
  public Set<BatchWindow> assignWindows(T input) {
    return Collections.singleton(BatchWindow.INSTANCE);
  }

  @SuppressWarnings("unchecked")
  public static <T> BatchWindowing<T> get() {
    return (BatchWindowing) INSTANCE;
  }

}
