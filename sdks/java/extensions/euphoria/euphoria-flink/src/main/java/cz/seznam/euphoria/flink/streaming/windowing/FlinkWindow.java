/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    @SuppressWarnings("unchecked")
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
