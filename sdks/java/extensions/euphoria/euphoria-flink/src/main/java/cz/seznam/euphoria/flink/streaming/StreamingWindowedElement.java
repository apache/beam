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
  /** The watermark when the window this element is part of was emitted. */
  private long emissionWatermark = Long.MIN_VALUE;

  public StreamingWindowedElement(W window, T element) {
    super(window, element);
  }

  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  public StreamingWindowedElement<W, T> withEmissionWatermark(long watermark) {
    this.emissionWatermark = watermark;
    return this;
  }
}
