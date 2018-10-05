package cz.seznam.euphoria.core.client.dataset.windowing;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class Batch<T>
    implements Windowing<T, Batch.Label, Batch.BatchWindowContext> {

  public static final class Label implements Serializable, Comparable<Label> {
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

    @Override
    public int compareTo(Label o) {
      return 0;
    }
  } // ~ end of Label

  public static class BatchWindowContext extends WindowContext<Label> {

    static final BatchWindowContext INSTANCE = new BatchWindowContext();
    static final Set<BatchWindowContext> INSTANCE_SET = Collections.singleton(INSTANCE);

    private BatchWindowContext() {
      super(new WindowID<>(Label.INSTANCE));
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }
  } // ~ end of BatchWindow

  private final static Batch<?> INSTANCE = new Batch<>();
  private Batch() {}

  @Override
  public Set<WindowID<Label>> assignWindowsToElement(
      WindowedElement<?, T> input) {
    return Collections.singleton(new WindowID<>(Label.INSTANCE));
  }

  @Override
  public BatchWindowContext createWindowContext(WindowID<Label> label) {
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
