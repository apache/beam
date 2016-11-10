package cz.seznam.euphoria.flink.streaming.windowing;


import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
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
  private transient long maxTimestamp = Long.MIN_VALUE;

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
    // see #overrideMaxTimestamp(long)
    if (maxTimestamp != Long.MIN_VALUE) {
      long mx = maxTimestamp;
      this.maxTimestamp = Long.MIN_VALUE;
      return mx;
    }

    if (this.wid instanceof TimedWindow) {
      return ((TimedWindow) this.wid).maxTimestamp();
    }
    return Long.MAX_VALUE;
  }

  // emh ... a temporary hack to override the value served by
  // maxTimestamp(); the value specified here will be served
  // the next time - and only the next time - maxTimestamp() is
  // called; this allows transferring the aligned time the window
  // was fired to the emitted elements:
  // see http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/WindowOperator-element-s-timestamp-td10038.html
  void overrideMaxTimestamp(long maxTimestamp) {
    this.maxTimestamp = maxTimestamp;
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
