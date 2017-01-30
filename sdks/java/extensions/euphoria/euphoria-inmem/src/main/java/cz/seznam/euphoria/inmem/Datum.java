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
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * Object passed inside inmem processing pipelines.
 * This is wrapper for
 *  * client data
 *  * end-of-stream marks
 *  * watermarks
 */
class Datum extends WindowedElement<Window, Object> {

  @SuppressWarnings("unchecked")
  static Datum of(Window window, Object element, long stamp) {
    return new Datum(window, element, stamp);
  }

  static Datum endOfStream() {
    return new EndOfStream();
  }
  
  static Datum watermark(long stamp) {
    return new Watermark(stamp);
  }

  @SuppressWarnings("unchecked")
  static Datum windowTrigger(Window window, long stamp) {
    return new WindowTrigger(window, stamp);
  }

  static class EndOfStream extends Datum {
    EndOfStream() {
      super(Long.MAX_VALUE);
    }
    @Override
    public boolean isEndOfStream() {
      return true;
    }
    @Override
    public String toString() {
      return "EndOfStream";
    }
  }

  static class Watermark extends Datum {
    Watermark(long stamp) {
      super(stamp);
    }
    @Override
    public boolean isWatermark() {
      return true;
    }
    @Override
    public String toString() {
      return "Watermark(" + getTimestamp() + ")";
    }
  }

  static class WindowTrigger extends Datum {
    @SuppressWarnings("unchecked")
    WindowTrigger(Window window, long stamp) {
      super(window, null, stamp);
    }
    @Override
    public boolean isWindowTrigger() {
      return true;
    }
    @Override
    public String toString() {
      return "WindowTrigger(" + getWindow() + ", " + getTimestamp() + ")";
    }
  }

  private Datum(long stamp) {
    super(null, stamp, null);
  }

  private Datum(Window window, Object element, long stamp) {
    super(window, stamp,  element);
  }

  /** Is this regular element message? */
  public boolean isElement() {
    return getElement() != null;
  }

  /** Is this end-of-stream message? */
  public boolean isEndOfStream() {
    return false;
  }

  /** Is this watermark message? */
  public boolean isWatermark() {
    return false;
  }

  /** Is this window trigger event? */
  public boolean isWindowTrigger() {
    return false;
  }

  @Override
  public String toString() {
    return "Datum(" + getWindow() + ", " + getTimestamp() + ", " + getElement() + ")";
  }

}
