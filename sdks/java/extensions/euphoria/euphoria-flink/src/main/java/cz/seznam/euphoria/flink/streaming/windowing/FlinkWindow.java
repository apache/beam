package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A presentation of {@link cz.seznam.euphoria.core.client.dataset.windowing.WindowID}
 * to Flink.
 */
public class FlinkWindow<GROUP, LABEL> extends Window
  implements WindowProperties<GROUP, LABEL>
{

  private final WindowID<GROUP, LABEL> windowID;

  private transient long emissionWatermark = Long.MIN_VALUE;

  public FlinkWindow(WindowContext<GROUP, LABEL> euphoriaContext) {
    this.windowID = euphoriaContext.getWindowID();
  }

  @Override
  public long getEmissionWatermark() {
    return emissionWatermark;
  }

  public void setEmissionWatermark(long emissionWatermark) {
    this.emissionWatermark = emissionWatermark;
  }

  @Override
  public long maxTimestamp() {
    // used for automatic cleanup - never without triggering
    return Long.MAX_VALUE;
  }

  @Override
  public WindowID<GROUP, LABEL> getWindowID() {
    return windowID;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof FlinkWindow)) return false;
    WindowID<?, ?> thatTindowID = ((FlinkWindow) obj).getWindowID();
    return thatTindowID.equals(this.getWindowID());
  }

  @Override
  public int hashCode() {    
    return windowID.hashCode();
  }

  @Override
  public String toString() {
    return "FlinkWindow{" +
        "windowID=" + windowID +
        ", emissionWatermark=" + emissionWatermark +
        '}';
  }
}
