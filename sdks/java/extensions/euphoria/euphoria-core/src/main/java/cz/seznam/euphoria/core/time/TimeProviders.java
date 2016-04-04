package cz.seznam.euphoria.core.time;

import java.lang.AssertionError;
import java.lang.Override;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

/**
 * A set of pre-defined time provider implementations all based on utilities
 * provided by the underlying JDK.
 */
public class TimeProviders {

  // ~ prevent instantiation
  private TimeProviders() {
    throw new AssertionError();
  }

  /**
   * Retrieves a time provider based on the default timezone
   */
  public static TimeProvider getInstance() {
    return new DefaultTimeProvider();
  }

  /**
   * Retrieves a time provider parametrized by the specified timezone
   *
   * @param tz timezone
   */
  public static TimeProvider getInstance(TimeZone tz) {
    return new TimezoneTimeProvider(tz);
  }

  /**
   * Retrieves a time provider with fixed datetime
   */
  public static FixedTimeProvider getFixedTimeInstance(Date d) {
    return new FixedTimeProvider(d);
  }

  public static abstract class AbstractTimeProvider implements TimeProvider {

    @Override
    public Date now() {
      return nowAsCalendar().getTime();
    }

    @Override
    public Date nowOffset(Duration offset) {
      return new Date(now().getTime() + offset.toMillis());
    }

    @Override
    public Date today() {
      Calendar now = nowAsCalendar();

      // reset hour, minutes, seconds and millis
      now.set(Calendar.HOUR_OF_DAY, 0);
      now.set(Calendar.MINUTE, 0);
      now.set(Calendar.SECOND, 0);
      now.set(Calendar.MILLISECOND, 0);

      return now.getTime();
    }

    public abstract Calendar nowAsCalendar();

  } // ~ end of AbstractTimeProvider

  /**
   * {@link TimeProvider} implementation based on the real system time
   * and default timezone
   */
  static class DefaultTimeProvider extends AbstractTimeProvider {
    @Override
    public Calendar nowAsCalendar() {
      return Calendar.getInstance();
    }
  } // ~ end of DefaultTimeProvider

  /**
   * {@link TimeProvider} implementation set to fixed time in UTC timezone.
   * Such a provider is useful for testing purposes.
   */
  public static class FixedTimeProvider extends AbstractTimeProvider {
    private volatile Date date;

    FixedTimeProvider(Date date) {
      this.date = Objects.requireNonNull(date);
    }

    public Date getFixedPoint() {
      return this.date;
    }

    public void setFixedPoint(Date d) {
      this.date = Objects.requireNonNull(d);
    }

    @Override
    public Calendar nowAsCalendar() {
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      c.setTime(date);
      return c;
    }
  } // ~ end of FixedTimeProvider

  /**
   * {@link TimeProvider} implementation based on the real system time
   * and given timezone
   */
  static class TimezoneTimeProvider extends AbstractTimeProvider {
    private final TimeZone timezone;

    public TimezoneTimeProvider(TimeZone timezone) {
      this.timezone = timezone;
    }

    @Override
    public Calendar nowAsCalendar() {
      return Calendar.getInstance(timezone);
    }
  } // ~ end of TimezoneTimeProvider
}