
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * Object passed inside inmem processing pipelines.
 * This is wrapper for
 *  * client data
 *  * end-of-stream marks
 *  * watermarks
 */
class Datum extends WindowedElement<Object, Object, Object> {

  @SuppressWarnings("unchecked")
  static Datum of(WindowID windowID, Object element) {
    return new Datum(windowID, element);
  }

  static Datum endOfStream() {
    return new EndOfStream();
  }
  
  static Datum watermark(long stamp) {
    return new Watermark(stamp);
  }

  @SuppressWarnings("unchecked")
  static Datum windowTrigger(WindowID windowID, long stamp) {
    return new WindowTrigger(windowID, stamp);
  }

  static class EndOfStream extends Datum {
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
    final long stamp;
    Watermark(long stamp) {
      this.stamp = stamp;
    }
    long getWatermark() {
      return stamp;
    }
    @Override
    public boolean isWatermark() {
      return true;
    }
    @Override
    public String toString() {
      return "Watermark(" + stamp + ")";
    }
  }

  static class WindowTrigger extends Datum {
    final long stamp;
    @SuppressWarnings("unchecked")
    WindowTrigger(WindowID windowID, long stamp) {
      super(windowID, null);
      this.stamp = stamp;
    }
    @Override
    public boolean isWindowTrigger() {
      return true;
    }
    public long getStamp() {
      return stamp;
    }
    @Override
    public String toString() {
      return "WindowTrigger(" + getWindowID() + ", " + stamp + ")";
    }
  }

  private Datum() {
    super(null, null);
  }

  private Datum(WindowID<Object, Object> windowID, Object element) {
    super(windowID, element);
  }


  /** Is this regular element message? */
  public boolean isElement() {
    return get() != null;
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


}
