package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * An extension to {@link WindowedElement} to carry flink specific information
 * along the elements. All elements flowing through the streaming flink executor are
 * of this type.
 */
public class StreamingWindowedElement<GROUP, LABEL, T>
    extends WindowedElement<GROUP, LABEL, T>
{
  /** The watermark when the window this element is part of was emitted. */
  private long emissionWatermark = -1;

  public StreamingWindowedElement(WindowID<GROUP, LABEL> windowID, T element) {
    super(windowID, element);
  }

  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  public StreamingWindowedElement<GROUP, LABEL, T> withEmissionWatermark(long watermark) {
    this.emissionWatermark = watermark;
    return this;
  }
}
