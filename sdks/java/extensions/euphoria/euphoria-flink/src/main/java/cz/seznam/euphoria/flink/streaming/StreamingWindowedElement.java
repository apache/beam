package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * An extension to {@link WindowedElement} to carry flink specific information
 * along the elements. All elements flowing through the streaming flink executor are
 * of this type.
 */
public class StreamingWindowedElement<LABEL, T>
    extends WindowedElement<LABEL, T>
    implements ElementProvider<T>
{
  /** The watermark when the window this element is part of was emitted. */
  private long emissionWatermark = Long.MIN_VALUE;

  public StreamingWindowedElement(WindowID<LABEL> windowID, T element) {
    super(windowID, element);
  }

  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  public StreamingWindowedElement<LABEL, T> withEmissionWatermark(long watermark) {
    this.emissionWatermark = watermark;
    return this;
  }
}
