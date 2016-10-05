package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Objects;

public class AttachedWindow<LABEL>
    extends Window
    implements WindowProperties<LABEL> {

  private final WindowID<LABEL> id;
  private final long emissionWatermark;

  public AttachedWindow(StreamingWindowedElement<LABEL, ?> element) {
    this(element.getWindowID(), element.getEmissionWatermark());
  }

  public AttachedWindow(WindowID<LABEL> id, long emissionWatermark) {
    this.id = Objects.requireNonNull(id);
    this.emissionWatermark = emissionWatermark;
  }

  @Override
  public WindowID<LABEL> getWindowID() {
    return id;
  }

  @Override
  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  @Override
  public long maxTimestamp() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof AttachedWindow) {
      AttachedWindow<?> that = (AttachedWindow<?>) o;
      return this.id.equals(that.id);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return "AttachedWindow{" +
        "id=" + id +
        ", emissionWatermark=" + emissionWatermark +
        '}';
  }
}
