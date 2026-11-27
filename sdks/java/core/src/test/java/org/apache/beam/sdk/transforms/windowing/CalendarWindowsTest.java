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

import static org.apache.beam.sdk.testing.WindowFnTestUtils.assignedWindows;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.CalendarWindows.MonthsWindows;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for CalendarWindows WindowFn. */
@RunWith(JUnit4.class)
public class CalendarWindowsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static Instant makeTimestamp(int year, int month, int day, int hours, int minutes) {
    return new DateTime(year, month, day, hours, minutes, DateTimeZone.UTC).toInstant();
  }

  /** Asserts that {@code windowFn} assigns each of {@code timestamps} to exactly {@code window}. */
  private static void assertWindow(
      WindowFn<Object, IntervalWindow> windowFn, IntervalWindow window, Instant... timestamps)
      throws Exception {
    for (Instant timestamp : timestamps) {
      assertThat(
          "windows for timestamp " + timestamp,
          assignedWindows(windowFn, timestamp.getMillis()),
          contains(window));
    }
  }

  @Test
  public void testDays() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn = CalendarWindows.days(1);
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 1, 1, 0, 0), makeTimestamp(2014, 1, 2, 0, 0)),
        makeTimestamp(2014, 1, 1, 0, 0),
        makeTimestamp(2014, 1, 1, 23, 59));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 1, 2, 0, 0), makeTimestamp(2014, 1, 3, 0, 0)),
        makeTimestamp(2014, 1, 2, 0, 0),
        makeTimestamp(2014, 1, 2, 5, 5));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2015, 1, 1, 0, 0), makeTimestamp(2015, 1, 2, 0, 0)),
        makeTimestamp(2015, 1, 1, 0, 0),
        makeTimestamp(2015, 1, 1, 5, 5));
  }

  @Test
  public void testDaysCompatibility() throws IncompatibleWindowException {
    CalendarWindows.DaysWindows daysWindows = CalendarWindows.days(10);
    daysWindows.verifyCompatibility(CalendarWindows.days(10));
    thrown.expect(IncompatibleWindowException.class);
    daysWindows.verifyCompatibility(CalendarWindows.days(9));
  }

  @Test
  public void testWeeks() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn =
        CalendarWindows.weeks(1, DateTimeConstants.WEDNESDAY);
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 1, 1, 0, 0), makeTimestamp(2014, 1, 8, 0, 0)),
        makeTimestamp(2014, 1, 1, 0, 0),
        makeTimestamp(2014, 1, 5, 5, 5));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 1, 8, 0, 0), makeTimestamp(2014, 1, 15, 0, 0)),
        makeTimestamp(2014, 1, 8, 0, 0),
        makeTimestamp(2014, 1, 12, 5, 5));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 12, 31, 0, 0), makeTimestamp(2015, 1, 7, 0, 0)),
        makeTimestamp(2015, 1, 1, 0, 0),
        makeTimestamp(2015, 1, 6, 5, 5));
  }

  @Test
  public void testMonths() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn = CalendarWindows.months(1);
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 1, 1, 0, 0), makeTimestamp(2014, 2, 1, 0, 0)),
        makeTimestamp(2014, 1, 1, 0, 0),
        makeTimestamp(2014, 1, 31, 5, 5));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 2, 1, 0, 0), makeTimestamp(2014, 3, 1, 0, 0)),
        makeTimestamp(2014, 2, 1, 0, 0),
        makeTimestamp(2014, 2, 15, 5, 5));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2015, 1, 1, 0, 0), makeTimestamp(2015, 2, 1, 0, 0)),
        makeTimestamp(2015, 1, 1, 0, 0),
        makeTimestamp(2015, 1, 31, 5, 5));
  }

  @Test
  public void testMonthsCompatibility() throws IncompatibleWindowException {
    CalendarWindows.MonthsWindows monthsWindows = CalendarWindows.months(10).beginningOnDay(15);
    monthsWindows.verifyCompatibility(CalendarWindows.months(10).beginningOnDay(15));
    thrown.expect(IncompatibleWindowException.class);
    monthsWindows.verifyCompatibility(CalendarWindows.months(10).beginningOnDay(30));
  }

  @Test
  public void testMultiMonths() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn =
        CalendarWindows.months(7).withStartingMonth(2014, 3).beginningOnDay(5);
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 3, 5, 0, 0), makeTimestamp(2014, 10, 5, 0, 0)),
        makeTimestamp(2014, 3, 5, 0, 0),
        makeTimestamp(2014, 10, 4, 23, 59));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2014, 10, 5, 0, 0), makeTimestamp(2015, 5, 5, 0, 0)),
        makeTimestamp(2014, 10, 5, 0, 0),
        makeTimestamp(2015, 3, 1, 0, 0));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2015, 12, 5, 0, 0), makeTimestamp(2016, 7, 5, 0, 0)),
        makeTimestamp(2016, 1, 5, 0, 0),
        makeTimestamp(2016, 1, 31, 5, 5));
  }

  @Test
  public void testYears() throws Exception {
    WindowFn<Object, IntervalWindow> windowFn =
        CalendarWindows.years(10).withStartingYear(2000).beginningOnDay(5, 5);
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2000, 5, 5, 0, 0), makeTimestamp(2010, 5, 5, 0, 0)),
        makeTimestamp(2000, 5, 5, 0, 0),
        makeTimestamp(2010, 5, 4, 23, 59));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2010, 5, 5, 0, 0), makeTimestamp(2020, 5, 5, 0, 0)),
        makeTimestamp(2010, 5, 5, 0, 0),
        makeTimestamp(2015, 3, 1, 0, 0));
    assertWindow(
        windowFn,
        new IntervalWindow(makeTimestamp(2050, 5, 5, 0, 0), makeTimestamp(2060, 5, 5, 0, 0)),
        makeTimestamp(2052, 1, 5, 0, 0),
        makeTimestamp(2060, 5, 4, 5, 5));
  }

  @Test
  public void testYearsCompatibility() throws IncompatibleWindowException {
    CalendarWindows.YearsWindows yearsWindows = CalendarWindows.years(2017).beginningOnDay(1, 1);
    yearsWindows.verifyCompatibility(CalendarWindows.years(2017).beginningOnDay(1, 1));
    thrown.expect(IncompatibleWindowException.class);
    yearsWindows.verifyCompatibility(CalendarWindows.years(2017).beginningOnDay(1, 2));
  }

  @Test
  public void testTimeZone() throws Exception {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    WindowFn<Object, IntervalWindow> windowFn = CalendarWindows.days(1).withTimeZone(timeZone);
    assertWindow(
        windowFn,
        new IntervalWindow(
            new DateTime(2014, 1, 1, 0, 0, timeZone).toInstant(),
            new DateTime(2014, 1, 2, 0, 0, timeZone).toInstant()),
        new DateTime(2014, 1, 1, 0, 0, timeZone).toInstant(),
        new DateTime(2014, 1, 1, 23, 59, timeZone).toInstant());
    assertWindow(
        windowFn,
        new IntervalWindow(
            new DateTime(2014, 1, 2, 0, 0, timeZone).toInstant(),
            new DateTime(2014, 1, 3, 0, 0, timeZone).toInstant()),
        new DateTime(2014, 1, 2, 8, 0, DateTimeZone.UTC).toInstant(),
        new DateTime(2014, 1, 3, 7, 59, DateTimeZone.UTC).toInstant());
  }

  @Test
  public void testDefaultWindowMappingFn() {
    MonthsWindows windowFn = CalendarWindows.months(2);
    WindowMappingFn<?> mapping = windowFn.getDefaultWindowMappingFn();

    assertThat(
        mapping.getSideInputWindow(
            new BoundedWindow() {
              @Override
              public Instant maxTimestamp() {
                return new Instant(100L);
              }
            }),
        equalTo(windowFn.assignWindow(new Instant(100L))));
    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }

  @Test
  public void testDefaultWindowMappingFnGlobal() {
    MonthsWindows windowFn = CalendarWindows.months(2);
    WindowMappingFn<?> mapping = windowFn.getDefaultWindowMappingFn();

    thrown.expect(IllegalArgumentException.class);
    mapping.getSideInputWindow(GlobalWindow.INSTANCE);
  }

  @Test
  public void testDisplayData() {
    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    Instant jan1 = new DateTime(1990, 1, 1, 0, 0, timeZone).toInstant();

    CalendarWindows.DaysWindows daysWindow =
        CalendarWindows.days(5).withStartingDay(1990, 1, 1).withTimeZone(timeZone);
    DisplayData daysDisplayData = DisplayData.from(daysWindow);
    assertThat(daysDisplayData, hasDisplayItem("numDays", 5));
    assertThat(daysDisplayData, hasDisplayItem("startDate", jan1));

    CalendarWindows.MonthsWindows monthsWindow =
        CalendarWindows.months(2).withStartingMonth(1990, 1).withTimeZone(timeZone);
    DisplayData monthsDisplayData = DisplayData.from(monthsWindow);
    assertThat(monthsDisplayData, hasDisplayItem("numMonths", 2));
    assertThat(monthsDisplayData, hasDisplayItem("startDate", jan1));

    CalendarWindows.YearsWindows yearsWindow =
        CalendarWindows.years(4).withStartingYear(1990).withTimeZone(timeZone);
    DisplayData yearsDisplayData = DisplayData.from(yearsWindow);
    assertThat(yearsDisplayData, hasDisplayItem("numYears", 4));
    assertThat(yearsDisplayData, hasDisplayItem("startDate", jan1));
  }
}
