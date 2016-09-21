package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A presentation of {@link cz.seznam.euphoria.core.client.dataset.windowing.WindowID}
 * to Flink.
 */
public class FlinkWindow extends Window {

  private final WindowID<?, ?> windowID;

  public FlinkWindow(WindowContext<?, ?> euphoriaContext) {
    this.windowID = euphoriaContext.getWindowID();
  }

  @Override
  public long maxTimestamp() {
    // used for automatic cleanup - never without triggering
    return Long.MAX_VALUE;
  }

  public WindowID<?, ?> getWindowID() {
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

}
