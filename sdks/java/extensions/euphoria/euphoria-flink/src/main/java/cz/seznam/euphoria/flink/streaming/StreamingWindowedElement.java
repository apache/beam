package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * An extension to {@link WindowedElement} to carry flink specific information
 * along the elements. All elements flowing through the streaming flink executor are
 * of this type.
 */
public class StreamingWindowedElement<W extends Window, T>
    extends WindowedElement<W, T>
    implements ElementProvider<T>
{
  public StreamingWindowedElement(W window, long timestamp, T element) {
    super(window, timestamp, element);
  }
}
