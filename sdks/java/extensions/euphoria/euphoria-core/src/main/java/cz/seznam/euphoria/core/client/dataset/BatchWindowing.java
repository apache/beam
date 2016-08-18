package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.executor.TriggerScheduler;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class BatchWindowing<T>
    implements AlignedWindowing<T, BatchWindowing.Batch, BatchWindowing.BatchWindowContext>
{
  public static final class Batch implements Serializable {
    static final Batch INSTANCE = new Batch();

    public static Batch get() { return INSTANCE; }

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

  public static class BatchWindowContext extends WindowContext<Void, Batch> {

    static final BatchWindowContext INSTANCE = new BatchWindowContext();
    static final Set<BatchWindowContext> INSTANCE_SET = Collections.singleton(INSTANCE);

    private BatchWindowContext() {
      super(WindowID.aligned(Batch.INSTANCE));
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of BatchWindow

  private final static BatchWindowing<?> INSTANCE = new BatchWindowing<>();
  private BatchWindowing() {}

  @Override
  public Set<WindowID<Void, Batch>> assignWindows(T input) {
    return Collections.singleton(WindowID.aligned(Batch.INSTANCE));
  }

  @Override
  public void updateTriggering(TriggerScheduler triggering, T input) {
    // ~ no-op; batch windows are not registering any triggers
  }

  @Override
  public BatchWindowContext createWindowContext(WindowID<Void, Batch> label) {
    return BatchWindowContext.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public static <T> BatchWindowing<T> get() {
    return (BatchWindowing) INSTANCE;
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
