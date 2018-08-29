/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.core.time;

import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/**
 * A set of pre-defined time provider implementations all based on utilities provided by the
 * underlying JDK.
 */
@Audience(Audience.Type.EXECUTOR)
public class TimeProviders {

  // ~ prevent instantiation
  private TimeProviders() {
    throw new AssertionError();
  }

  /**
   * Retrieves a time provider based on the default timezone.
   *
   * @return a default time provider based on the jvm's "current" time zone and the machines local
   *     datetime
   */
  public static TimeProvider getInstance() {
    return new DefaultTimeProvider();
  }

  /**
   * Retrieves a time provider parametrized by the specified timezone.
   *
   * @param tz timezone
   * @return a time provider based on the specified time zone
   */
  public static TimeProvider getInstance(TimeZone tz) {
    return new TimezoneTimeProvider(tz);
  }

  /**
   * Retrieves a time provider with fixed datetime.
   *
   * @param d the fixed point in time
   * @return a time provider based on the a fixed point in time
   */
  public static FixedTimeProvider getFixedTimeInstance(Date d) {
    return new FixedTimeProvider(d);
  }

  /** TODO: complete javadoc. */
  public abstract static class AbstractTimeProvider implements TimeProvider {

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

  /** {@link TimeProvider} implementation based on the real system time and default timezone. */
  static class DefaultTimeProvider extends AbstractTimeProvider {
    @Override
    public Calendar nowAsCalendar() {
      return Calendar.getInstance();
    }
  } // ~ end of DefaultTimeProvider

  /**
   * {@link TimeProvider} implementation set to fixed time in UTC timezone. Such a provider is
   * useful for testing purposes.
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

  /** {@link TimeProvider} implementation based on the real system time and given timezone. */
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
