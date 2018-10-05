package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.executor.TriggerScheduler;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class Batch<T>
    implements AlignedWindowing<T, Batch.Label, Batch.BatchWindowContext>
{
  public static final class Label implements Serializable {
    static final Label INSTANCE = new Label();

    public static Label get() { return INSTANCE; }

    private Label() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof Label;
    }

    @Override
    public int hashCode() {
      return Integer.MAX_VALUE;
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of Label

  public static class BatchWindowContext extends WindowContext<Void, Label> {

    static final BatchWindowContext INSTANCE = new BatchWindowContext();
    static final Set<BatchWindowContext> INSTANCE_SET = Collections.singleton(INSTANCE);

    private BatchWindowContext() {
      super(WindowID.aligned(Label.INSTANCE));
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of BatchWindow

  private final static Batch<?> INSTANCE = new Batch<>();
  private Batch() {}

  @Override
  public Set<WindowID<Void, Label>> assignWindows(T input) {
    return Collections.singleton(WindowID.aligned(Label.INSTANCE));
  }

  @Override
  public void updateTriggering(TriggerScheduler triggering, T input) {
    // ~ no-op; batch windows are not registering any triggers
  }

  @Override
  public BatchWindowContext createWindowContext(WindowID<Void, Label> label) {
    return BatchWindowContext.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  public static <T> Batch<T> get() {
    return (Batch) INSTANCE;
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
