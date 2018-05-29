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
package org.apache.beam.sdk.transforms.windowing;

import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.Months;
import org.joda.time.Years;

/**
 * A collection of {@link WindowFn}s that windows values into calendar-based
 * windows such as spans of days, months, or years.
 *
 * <p>For example, to group data into quarters that change on the 15th, use
 * {@code CalendarWindows.months(3).withStartingMonth(2014, 1).beginningOnDay(15)}.
 */
public class CalendarWindows {

  private static final DateTime DEFAULT_START_DATE = new DateTime(0, DateTimeZone.UTC);

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by days.
   *
   * <p>For example, {@code CalendarWindows.days(1)} will window elements into
   * separate windows for each day.
   */
  public static DaysWindows days(int number) {
    return new DaysWindows(number, DEFAULT_START_DATE, DateTimeZone.UTC);
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by weeks.
   *
   * <p>For example, {@code CalendarWindows.weeks(1, DateTimeConstants.TUESDAY)} will
   * window elements into week-long windows starting on Tuesdays.
   */
  public static DaysWindows weeks(int number, int startDayOfWeek) {
    return new DaysWindows(
        7 * number,
        DEFAULT_START_DATE.withDayOfWeek(startDayOfWeek),
        DateTimeZone.UTC);
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by months.
   *
   * <p>For example,
   * {@code CalendarWindows.months(8).withStartingMonth(2014, 1).beginningOnDay(10)}
   * will window elements into 8 month windows where that start on the 10th day of month,
   * and the first window begins in January 2014.
   */
  public static MonthsWindows months(int number) {
    return new MonthsWindows(number, 1, DEFAULT_START_DATE, DateTimeZone.UTC);
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by years.
   *
   * <p>For example,
   * {@code CalendarWindows.years(1).withTimeZone(DateTimeZone.forId("America/Los_Angeles"))}
   * will window elements into year-long windows that start at midnight on Jan 1, in the
   * America/Los_Angeles time zone.
   */
  public static YearsWindows years(int number) {
    return new YearsWindows(number, 1, 1, DEFAULT_START_DATE, DateTimeZone.UTC);
  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by days.
   *
   * <p>By default, periods of multiple days are measured starting at the
   * epoch.  This can be overridden with {@link #withStartingDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this
   * is overridden with the {@link #withTimeZone} method.
   */
  public static class DaysWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public DaysWindows withStartingDay(int year, int month, int day) {
      return new DaysWindows(
          number, new DateTime(year, month, day, 0, 0, timeZone), timeZone);
    }

    public DaysWindows withTimeZone(DateTimeZone timeZone) {
      return new DaysWindows(
          number, startDate.withZoneRetainFields(timeZone), timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private int number;
    private DateTime startDate;
    private DateTimeZone timeZone;

    private DaysWindows(int number, DateTime startDate, DateTimeZone timeZone) {
      this.number = number;
      this.startDate = startDate;
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {
      DateTime datetime = new DateTime(timestamp, timeZone);

      int dayOffset = Days.daysBetween(startDate, datetime).getDays() / number * number;

      DateTime begin = startDate.plusDays(dayOffset);
      DateTime end = begin.plusDays(number);

      return new IntervalWindow(begin.toInstant(), end.toInstant());
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof DaysWindows)) {
        return false;
      }
      DaysWindows that = (DaysWindows) other;
      return number == that.number
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of days, start date "
                    + "and time zone are compatible.",
                DaysWindows.class.getSimpleName()));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .add(DisplayData.item("numDays", number)
            .withLabel("Windows Days"))
          .addIfNotDefault(
              DisplayData.item("startDate", new DateTime(startDate, timeZone).toInstant())
                .withLabel("Window Start Date"),
              new DateTime(DEFAULT_START_DATE, DateTimeZone.UTC).toInstant());
    }

    public int getNumber() {
      return number;
    }

    public DateTime getStartDate() {
      return startDate;
    }

    public DateTimeZone getTimeZone() {
      return timeZone;
    }

  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by months.
   *
   * <p>By default, periods of multiple months are measured starting at the
   * epoch.  This can be overridden with {@link #withStartingMonth}.
   *
   * <p>Months start on the first day of each calendar month, unless overridden by
   * {@link #beginningOnDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this
   * is overridden with the {@link #withTimeZone} method.
   */
  public static class MonthsWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public MonthsWindows beginningOnDay(int dayOfMonth) {
      return new MonthsWindows(
          number, dayOfMonth, startDate, timeZone);
    }

    public MonthsWindows withStartingMonth(int year, int month) {
      return new MonthsWindows(
          number, dayOfMonth, new DateTime(year, month, 1, 0, 0, timeZone), timeZone);
    }

    public MonthsWindows withTimeZone(DateTimeZone timeZone) {
      return new MonthsWindows(
          number, dayOfMonth, startDate.withZoneRetainFields(timeZone), timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private int number;
    private int dayOfMonth;
    private DateTime startDate;
    private DateTimeZone timeZone;

    private MonthsWindows(int number, int dayOfMonth, DateTime startDate, DateTimeZone timeZone) {
      this.number = number;
      this.dayOfMonth = dayOfMonth;
      this.startDate = startDate;
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {
      DateTime datetime = new DateTime(timestamp, timeZone);

      int monthOffset =
          Months.monthsBetween(startDate.withDayOfMonth(dayOfMonth), datetime).getMonths()
          / number * number;

      DateTime begin = startDate.withDayOfMonth(dayOfMonth).plusMonths(monthOffset);
      DateTime end = begin.plusMonths(number);

      return new IntervalWindow(begin.toInstant(), end.toInstant());
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof MonthsWindows)) {
        return false;
      }
      MonthsWindows that = (MonthsWindows) other;
      return number == that.number
          && dayOfMonth == that.dayOfMonth
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of months, "
                    + "day of month, start date and time zone are compatible.",
                MonthsWindows.class.getSimpleName()));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .add(DisplayData.item("numMonths", number)
            .withLabel("Window Months"))
          .addIfNotDefault(
            DisplayData.item("startDate", new DateTime(startDate, timeZone).toInstant())
              .withLabel("Window Start Date"),
            new DateTime(DEFAULT_START_DATE, DateTimeZone.UTC).toInstant());
    }

    public int getNumber() {
      return number;
    }

    public int getDayOfMonth() {
      return dayOfMonth;
    }

    public DateTime getStartDate() {
      return startDate;
    }

    public DateTimeZone getTimeZone() {
      return timeZone;
    }

  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by years.
   *
   * <p>By default, periods of multiple years are measured starting at the
   * epoch.  This can be overridden with {@link #withStartingYear}.
   *
   * <p>Years start on the first day of each calendar year, unless overridden by
   * {@link #beginningOnDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this
   * is overridden with the {@link #withTimeZone} method.
   */
  public static class YearsWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public YearsWindows beginningOnDay(int monthOfYear, int dayOfMonth) {
      return new YearsWindows(
          number, monthOfYear, dayOfMonth, startDate, timeZone);
    }

    public YearsWindows withStartingYear(int year) {
      return new YearsWindows(
          number, monthOfYear, dayOfMonth, new DateTime(year, 1, 1, 0, 0, timeZone), timeZone);
    }

    public YearsWindows withTimeZone(DateTimeZone timeZone) {
      return new YearsWindows(
          number, monthOfYear, dayOfMonth, startDate.withZoneRetainFields(timeZone), timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private int number;
    private int monthOfYear;
    private int dayOfMonth;
    private DateTime startDate;
    private DateTimeZone timeZone;

    private YearsWindows(
        int number, int monthOfYear, int dayOfMonth, DateTime startDate, DateTimeZone timeZone) {
      this.number = number;
      this.monthOfYear = monthOfYear;
      this.dayOfMonth = dayOfMonth;
      this.startDate = startDate;
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(Instant timestamp) {
      DateTime datetime = new DateTime(timestamp, timeZone);

      DateTime offsetStart = startDate.withMonthOfYear(monthOfYear).withDayOfMonth(dayOfMonth);

      int yearOffset =
          Years.yearsBetween(offsetStart, datetime).getYears() / number * number;

      DateTime begin = offsetStart.plusYears(yearOffset);
      DateTime end = begin.plusYears(number);

      return new IntervalWindow(begin.toInstant(), end.toInstant());
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof YearsWindows)) {
        return false;
      }
      YearsWindows that = (YearsWindows) other;
      return number == that.number
          && monthOfYear == that.monthOfYear
          && dayOfMonth == that.dayOfMonth
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of years, month of year, "
                    + "day of month, start date and time zone are compatible.",
                YearsWindows.class.getSimpleName()));
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder
          .add(DisplayData.item("numYears", number)
            .withLabel("Window Years"))
          .addIfNotDefault(
              DisplayData.item("startDate", new DateTime(startDate, timeZone).toInstant())
                .withLabel("Window Start Date"),
              new DateTime(DEFAULT_START_DATE, DateTimeZone.UTC).toInstant());
    }

    public DateTimeZone getTimeZone() {
      return timeZone;
    }

    public DateTime getStartDate() {
      return startDate;
    }

    public int getDayOfMonth() {
      return dayOfMonth;
    }

    public int getMonthOfYear() {
      return monthOfYear;
    }

    public int getNumber() {
      return number;
    }

  }
}
