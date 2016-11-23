package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class Batch<T>
    implements Windowing<T, Batch.BatchWindow> {

  public static final class BatchWindow extends Window implements Comparable<BatchWindow> {
    static final BatchWindow INSTANCE = new BatchWindow();

    public static BatchWindow get() { return INSTANCE; }

    private BatchWindow() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof BatchWindow;
    }

    @Override
    public int hashCode() {
      return Integer.MAX_VALUE;
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }

    @Override
    public int compareTo(BatchWindow o) {
      return 0;
    }
  } // ~ end of Label

  private final static Batch<?> INSTANCE = new Batch<>();
  private Batch() {}

  @Override
  public Set<BatchWindow> assignWindowsToElement(WindowedElement<?, T> el) {
    return Collections.singleton(BatchWindow.INSTANCE);
  }

  @Override
  public Trigger<BatchWindow> getTrigger() {
    return NoopTrigger.get();
  }

  @SuppressWarnings("unchecked")
  public static <T> Batch<T> get() {
    return (Batch) INSTANCE;
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
