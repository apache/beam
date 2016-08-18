package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.util.List;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class FlinkWindow extends Window {

  private final WindowContext<?, ?> euphoriaWindowContext;

  private boolean isOpened;
  private List<Trigger> triggers;

  public FlinkWindow(WindowContext<?, ?> euphoriaContext) {
    this.euphoriaWindowContext = euphoriaContext;
  }

  @Override
  public long maxTimestamp() {
    // used for automatic cleanup - never without triggering
    return Long.MAX_VALUE;
  }

  public WindowContext<?, ?> getWindowContext() {
    return euphoriaWindowContext;
  }

  /**
   * Initializes the window with the first element
   */
  public void open() {
    isOpened = true;
    triggers = euphoriaWindowContext.createTriggers();
  }

  /**
   * @return {@code TRUE} if the window have been already initialized
   */
  public boolean isOpened() {
    return isOpened;
  }

  public List<Trigger> getTriggers() {
    return triggers;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof FlinkWindow)) return false;
    WindowID<?, ?> windowID = ((FlinkWindow) obj).euphoriaWindowContext.getWindowID();
    return windowID.equals(this.euphoriaWindowContext.getWindowID());
  }

  @Override
  public int hashCode() {    
    return euphoriaWindowContext.getWindowID().hashCode();
  }


}
