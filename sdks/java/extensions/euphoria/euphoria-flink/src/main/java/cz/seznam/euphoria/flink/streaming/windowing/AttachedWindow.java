/**
 * Copyright 2016 Seznam a.s.
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


import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Objects;

public class AttachedWindow<WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window>
    extends Window
    implements WindowProperties<WID> {

  private final WID wid;

  // ~ transient on purpose not to affect serialized form
  // because RocksDB uses that form as a key in state backend
  private transient final long emissionWatermark;

  public AttachedWindow(StreamingWindowedElement<WID, ?> element) {
    this(element.getWindow(), element.getTimestamp());
  }

  public AttachedWindow(WID wid, long emissionWatermark) {
    this.wid = Objects.requireNonNull(wid);
    this.emissionWatermark = emissionWatermark;
  }

  @Override
  public WID getWindowID() {
    return wid;
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
      return this.wid.equals(that.wid);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return wid.hashCode();
  }

  @Override
  public String toString() {
    return "AttachedWindow{" +
        "wid=" + wid +
        ", emissionWatermark=" + emissionWatermark +
        '}';
  }
}
