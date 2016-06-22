package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class BatchWindowing<T>
    implements AlignedWindowing<T, BatchWindowing.Batch, BatchWindowing.BatchWindow>
{
  public static final class Batch implements Serializable {
    static final Batch INSTANCE = new Batch();

    private Batch() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof Batch;
    }

    @Override
    public int hashCode() {
      return Integer.MAX_VALUE;
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of Batch

  public static class BatchWindow implements AlignedWindow<Batch> {

    static final BatchWindow INSTANCE = new BatchWindow();
    static final Set<BatchWindow> INSTANCE_SET = Collections.singleton(INSTANCE);

    private BatchWindow() {}

    @Override
    public Batch getLabel() {
      return Batch.INSTANCE;
    }

    @Override
    public List<Trigger> createTriggers() {
      return Collections.emptyList();
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of BatchWindow

  private final static BatchWindowing<?> INSTANCE = new BatchWindowing<>();
  private BatchWindowing() {}

  @Override
  public Set<BatchWindow> assignWindows(T input) {
    return BatchWindow.INSTANCE_SET;
  }

  @SuppressWarnings("unchecked")
  public static <T> BatchWindowing<T> get() {
    return (BatchWindowing) INSTANCE;
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
