
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import java.util.Objects;

/**
 * Object passed inside inmem processing pipelines.
 * This is wrapper for
 *  * client data
 *  * end-of-stream marks
 *  * watermarks
 */
class Datum extends WindowedElement<Object, Object, Object> {

  @SuppressWarnings("unchecked, rawtypes")
  static Datum of(WindowID windowID, Object element) {
    return new Datum(windowID, element);
  }

  static Datum endOfStream() {
    return new EndOfStream();
  }
  
  static Datum watermark(long stamp) {
    return new Watermark(stamp);
  }

  @SuppressWarnings("unchecked, rawtypes")
  static Datum windowTrigger(WindowID windowID, long stamp) {
    return new WindowTrigger(windowID, stamp);
  }

  static class EndOfStream extends Datum {
    @Override
    public boolean isEndOfStream() {
      return true;
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
  }

  static class WindowTrigger extends Datum {
    final long stamp;
    final WindowID windowID;
    WindowTrigger(WindowID windowID, long stamp) {
      this.stamp = stamp;
      this.windowID = windowID;
    }
    @Override
    public boolean isWindowTrigger() {
      return true;
    }
    public long getStamp() {
      return stamp;
    }
  }

  private Datum() {
    super(null, null);
  }

  private Datum(WindowID<Object, Object> windowID, Object element) {
    super(windowID, Objects.requireNonNull(element));
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

  /**
   * Is this a broadcast message (message that has to be replicated to all
   *  downstream partitions?
   **/
  public boolean isBroadcast() {
    // this is true following
    return isWatermark() || isEndOfStream() || isWindowTrigger();
  }


}
