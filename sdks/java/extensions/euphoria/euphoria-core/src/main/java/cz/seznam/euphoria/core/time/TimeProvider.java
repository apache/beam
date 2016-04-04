package cz.seznam.euphoria.core.time;

import java.io.Serializable;
import java.time.Duration;
import java.util.Date;

/**
 * A provider of the "current" timestamp aiming to decouple
 * the "current" time consumers and their corresponding producers.
 */
public interface TimeProvider extends Serializable {

  /**
   * Retrieves the current moment of time (timestamp of "now")
   *
   * @return the "current" timestamp; never {@code null}
   */
  Date now();

  /**
   * Retrieves he current moment of time (timestamp of "now")
   * with given offset added
   *
   * @param offset offset in milliseconds
   * @return the "current" timestamp with offset; never {@code null}
   */
  Date nowOffset(Duration offset);

  /**
   * Retrieves the current moment of time with a precision of a day.
   * This is basically the timestamp of "today's" midnight.
   *
   * @return the "current" date; never {@code null}
   */
  Date today();
}
