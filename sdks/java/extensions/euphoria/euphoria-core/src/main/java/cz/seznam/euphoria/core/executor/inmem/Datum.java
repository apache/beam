
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
class Datum extends WindowedElement<Object, Object> {

  @SuppressWarnings("unchecked")
  static Datum of(WindowID windowID, Object element, long stamp) {
    return new Datum(windowID, element, stamp);
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
      return "Watermark(" + stamp + ")";
    }
  }

  static class WindowTrigger extends Datum {
    @SuppressWarnings("unchecked")
    WindowTrigger(WindowID windowID, long stamp) {
      super(windowID, null, stamp);
    }
    @Override
    public boolean isWindowTrigger() {
      return true;
    }
    @Override
    public String toString() {
      return "WindowTrigger(" + getWindowID() + ", " + stamp + ")";
    }
  }

  // timestamp of the event
  long stamp;

  private Datum(long stamp) {
    super(null, null);
    this.stamp = stamp;
  }

  private Datum(WindowID<Object> windowID, Object element, long stamp) {
    super(windowID, element);
    this.stamp = stamp;
  }

  /** Get timestamp of the event. */
  public long getStamp() {
    return stamp;
  }

  void setStamp(long stamp) {
    this.stamp = stamp;
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

  @Override
  public String toString() {
    return "Datum(" + this.getWindowID() + ", " + stamp + ", " + get() + ")";
  }




}
