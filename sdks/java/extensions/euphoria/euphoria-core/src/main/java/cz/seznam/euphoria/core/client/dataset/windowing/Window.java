package cz.seznam.euphoria.core.client.dataset.windowing;

import java.io.Serializable;

/**
 * A {@link Windowing} strategy associates each input element with a window
 * thereby grouping input elements into chunks
 * for further processing in small (micro-)batches.
 * <p>
 * Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 */
public abstract class Window implements Serializable {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);
}