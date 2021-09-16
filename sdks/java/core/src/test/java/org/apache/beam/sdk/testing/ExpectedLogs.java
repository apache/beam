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
package org.apache.beam.sdk.testing;

import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.annotation.concurrent.ThreadSafe;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

/**
 * This {@link TestRule} enables the ability to capture JUL logging events during test execution and
 * assert expectations that they contain certain messages (with or without {@link Throwable}) at
 * certain log levels. For logs generated via the SLF4J logging frontend, the JUL backend must be
 * used.
 */
public class ExpectedLogs extends ExternalResource {
  /**
   * Returns a {@link TestRule} that captures logs for the given logger name.
   *
   * @param name The logger name to capture logs for.
   * @return A {@link ExpectedLogs} test rule.
   */
  public static ExpectedLogs none(String name) {
    return new ExpectedLogs(name);
  }

  /**
   * Returns a {@link TestRule} that captures logs for the given class.
   *
   * @param klass The class to capture logs for.
   * @return A {@link ExpectedLogs} test rule.
   */
  public static ExpectedLogs none(Class<?> klass) {
    return ExpectedLogs.none(klass.getName());
  }

  /**
   * Verify a logging event at the trace level with the given message.
   *
   * @param substring The message to match against.
   */
  public void verifyTrace(String substring) {
    verify(Level.FINEST, substring);
  }

  /**
   * Verify a logging event at the trace level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyTrace(String substring, Throwable t) {
    verify(Level.FINEST, substring, t);
  }

  /**
   * Verify a logging event at the debug level with the given message.
   *
   * @param substring The message to match against.
   */
  public void verifyDebug(String substring) {
    verify(Level.FINE, substring);
  }

  /**
   * Verify a logging event at the debug level with the given message and throwable.
   *
   * @param message The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyDebug(String message, Throwable t) {
    verify(Level.FINE, message, t);
  }

  /**
   * Verify a logging event at the info level with the given message.
   *
   * @param substring The message to match against.
   */
  public void verifyInfo(String substring) {
    verify(Level.INFO, substring);
  }

  /**
   * Verify a logging event at the info level with the given message and throwable.
   *
   * @param message The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyInfo(String message, Throwable t) {
    verify(Level.INFO, message, t);
  }

  /**
   * Verify a logging event at the warn level with the given message.
   *
   * @param substring The message to match against.
   */
  public void verifyWarn(String substring) {
    verify(Level.WARNING, substring);
  }

  /**
   * Verify a logging event at the warn level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyWarn(String substring, Throwable t) {
    verify(Level.WARNING, substring, t);
  }

  /**
   * Verify a logging event at the error level with the given message.
   *
   * @param substring The message to match against.
   */
  public void verifyError(String substring) {
    verify(Level.SEVERE, substring);
  }

  /**
   * Verify a logging event at the error level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyError(String substring, Throwable t) {
    verify(Level.SEVERE, substring, t);
  }

  /**
   * Verify there are no logging events with messages containing the given substring.
   *
   * @param substring The message to match against.
   */
  public void verifyNotLogged(String substring) {
    verifyNotLogged(matcher(substring));
  }

  /**
   * Verify there is no logging event at the error level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void verifyNoError(String substring, Throwable t) {
    verifyNo(Level.SEVERE, substring, t);
  }

  /**
   * Verify that the list of log records matches the provided {@code matcher}.
   *
   * @param matcher The matcher to use.
   */
  public void verifyLogRecords(Matcher<Iterable<LogRecord>> matcher) {
    if (!matcher.matches(logSaver.getLogs())) {
      fail(String.format("Missing match for [%s]", matcher));
    }
  }

  private void verify(final Level level, final String substring) {
    verifyLogged(matcher(level, substring));
  }

  private TypeSafeMatcher<LogRecord> matcher(final String substring) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format("log message containing message [%s]", substring));
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return item.getMessage().contains(substring);
      }
    };
  }

  private TypeSafeMatcher<LogRecord> matcher(final Level level, final String substring) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format("log message of level [%s] containing message [%s]", level, substring));
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return level.equals(item.getLevel()) && item.getMessage().contains(substring);
      }
    };
  }

  private void verify(final Level level, final String substring, final Throwable throwable) {
    verifyLogged(matcher(level, substring, throwable));
  }

  private void verifyNo(final Level level, final String substring, final Throwable throwable) {
    verifyNotLogged(matcher(level, substring, throwable));
  }

  private TypeSafeMatcher<LogRecord> matcher(
      final Level level, final String substring, final Throwable throwable) {
    return new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(
            String.format(
                "log message of level [%s] containg message [%s] with exception [%s] "
                    + "containing message [%s]",
                level, substring, throwable.getClass(), throwable.getMessage()));
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return level.equals(item.getLevel())
            && item.getMessage().contains(substring)
            && item.getThrown().getClass().equals(throwable.getClass())
            && item.getThrown().getMessage().contains(throwable.getMessage());
      }
    };
  }

  private void verifyLogged(Matcher<LogRecord> matcher) {
    for (LogRecord record : logSaver.getLogs()) {
      if (matcher.matches(record)) {
        return;
      }
    }

    fail(String.format("Missing match for [%s]", matcher));
  }

  private void verifyNotLogged(Matcher<LogRecord> matcher) {
    // Don't use Matchers.everyItem(Matchers.not(matcher)) because it doesn't format the logRecord
    for (LogRecord record : logSaver.getLogs()) {
      if (matcher.matches(record)) {
        fail(String.format("Unexpected match of [%s]: [%s]", matcher, logFormatter.format(record)));
      }
    }
  }

  @Override
  protected void before() throws Throwable {
    previousLevel = log.getLevel();
    log.setLevel(Level.ALL);
    log.addHandler(logSaver);
  }

  @Override
  protected void after() {
    log.removeHandler(logSaver);
    log.setLevel(previousLevel);
    logSaver.reset();
  }

  private final Logger log;
  private final LogSaver logSaver;
  private final Formatter logFormatter = new SimpleFormatter();
  private Level previousLevel;

  private ExpectedLogs(String name) {
    log = Logger.getLogger(name);
    logSaver = new LogSaver();
  }

  /** A JUL logging {@link Handler} that records all logging events that are passed to it. */
  @ThreadSafe
  private static class LogSaver extends Handler {
    private final Collection<LogRecord> logRecords = new ConcurrentLinkedDeque<>();

    @Override
    public void publish(LogRecord record) {
      logRecords.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}

    private Collection<LogRecord> getLogs() {
      return logRecords;
    }

    private void reset() {
      logRecords.clear();
    }
  }
}
