package cz.seznam.euphoria.flink.streaming.windowing;


import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * A presentation of {@link cz.seznam.euphoria.core.client.dataset.windowing.Window}
 * to Flink.
 */
public class FlinkWindow<WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window>
        extends Window implements WindowProperties<WID>
{

  private final WID wid;

  private transient long emissionWatermark = Long.MIN_VALUE;

  public FlinkWindow(WID wid) {
    this.wid = wid;
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
  public WID getWindowID() {
    return wid;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof FlinkWindow)) return false;
    WID thatTindowID = ((FlinkWindow<WID>) obj).getWindowID();
    return thatTindowID.equals(this.getWindowID());
  }

  @Override
  public int hashCode() {    
    return wid.hashCode();
  }

  @Override
  public String toString() {
    return "FlinkWindow{" +
        "wid=" + wid +
        ", emissionWatermark=" + emissionWatermark +
        '}';
  }
}
