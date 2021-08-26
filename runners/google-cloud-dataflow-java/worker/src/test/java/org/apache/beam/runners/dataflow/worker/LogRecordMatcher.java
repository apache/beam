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
package org.apache.beam.runners.dataflow.worker;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/** Hamcrest matcher for asserts on {@link LogRecord} instances. */
public final class LogRecordMatcher extends TypeSafeMatcher<LogRecord> {
  private final String substring;
  private final Matcher<Level> levelMatcher;

  private LogRecordMatcher(Level level, String substring) {
    this(Matchers.is(level), substring);
  }

  /**
   * Match {@link LogRecord} instances with the specified level, and containing the specified
   * message substring.
   */
  public static LogRecordMatcher hasLog(Level level, String substring) {
    return new LogRecordMatcher(level, substring);
  }

  /** Match {@link LogRecord} instances containing the specified message substring. */
  public static LogRecordMatcher hasLog(String substring) {
    return new LogRecordMatcher(Matchers.any(Level.class), substring);
  }

  /**
   * Match a {@link LogRecord} in the specified {@link Iterable} containing the specified message
   * substring.
   */
  public static Matcher<Iterable<? super LogRecord>> hasLogItem(String substring) {
    return Matchers.hasItem(hasLog(substring));
  }

  /**
   * Match a {@link LogRecord} in the specified {@link Iterable} with the specified level, and
   * containing the specified message substring.
   */
  public static Matcher<Iterable<? super LogRecord>> hasLogItem(Level level, String substring) {
    return Matchers.hasItem(hasLog(level, substring));
  }

  private LogRecordMatcher(Matcher<Level> levelMatcher, String substring) {
    this.levelMatcher = levelMatcher;
    this.substring = substring;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("level ");
    levelMatcher.describeTo(description);
    description.appendText(String.format(" and message containing <%s>", substring));
  }

  @Override
  public void describeMismatchSafely(LogRecord item, Description description) {
    description
        .appendText("was log with message \"")
        .appendText(item.getMessage())
        .appendText("\" at severity ")
        .appendValue(item.getLevel());
  }

  @Override
  protected boolean matchesSafely(LogRecord item) {
    return levelMatcher.matches(item.getLevel()) && item.getMessage().contains(substring);
  }
}
