/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import static org.junit.Assert.fail;

import com.google.common.collect.Lists;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This {@link TestRule} enables the ability to capture JUL logging events during test execution and
 * assert expectations that they contain certain messages (with or without {@link Throwable}) at
 * certain log levels. For logs generated via the SLF4J logging frontend, the JUL backend must be
 * used.
 */
public class ExpectedLogs extends ExternalResource {
  /**
   * Returns a {@link TestRule} which captures logs for the given class.
   *
   * @param klass The class to capture logs for.
   * @return A {@link ExpectedLogs} test rule.
   */
  public static ExpectedLogs none(Class<?> klass) {
    return new ExpectedLogs(klass);
  }

  /**
   * Expect a logging event at the trace level with the given message.
   *
   * @param substring The message to match against.
   */
  public void expectTrace(String substring) {
    expect(Level.FINEST, substring);
  }

  /**
   * Expect a logging event at the trace level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void expectTrace(String substring, Throwable t) {
    expect(Level.FINEST, substring, t);
  }

  /**
   * Expect a logging event at the debug level with the given message.
   *
   * @param substring The message to match against.
   */
  public void expectDebug(String substring) {
    expect(Level.FINE, substring);
  }

  /**
   * Expect a logging event at the debug level with the given message and throwable.
   *
   * @param message The message to match against.
   * @param t The throwable to match against.
   */
  public void expectDebug(String message, Throwable t) {
    expect(Level.FINE, message, t);
  }

  /**
   * Expect a logging event at the info level with the given message.
   * @param substring The message to match against.
   */
  public void expectInfo(String substring) {
    expect(Level.INFO, substring);
  }

  /**
   * Expect a logging event at the info level with the given message and throwable.
   *
   * @param message The message to match against.
   * @param t The throwable to match against.
   */
  public void expectInfo(String message, Throwable t) {
    expect(Level.INFO, message, t);
  }

  /**
   * Expect a logging event at the warn level with the given message.
   *
   * @param substring The message to match against.
   */
  public void expectWarn(String substring) {
    expect(Level.WARNING, substring);
  }

  /**
   * Expect a logging event at the warn level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void expectWarn(String substring, Throwable t) {
    expect(Level.WARNING, substring, t);
  }

  /**
   * Expect a logging event at the error level with the given message.
   *
   * @param substring The message to match against.
   */
  public void expectError(String substring) {
    expect(Level.SEVERE, substring);
  }

  /**
   * Expect a logging event at the error level with the given message and throwable.
   *
   * @param substring The message to match against.
   * @param t The throwable to match against.
   */
  public void expectError(String substring, Throwable t) {
    expect(Level.SEVERE, substring, t);
  }

  private void expect(final Level level, final String substring) {
    expectations.add(new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format(
            "Expected log message of level [%s] containing message [%s]", level, substring));
      }

      @Override
      protected boolean matchesSafely(LogRecord item) {
        return level.equals(item.getLevel())
            && item.getMessage().contains(substring);
      }
    });
  }

  private void expect(final Level level, final String substring, final Throwable throwable) {
    expectations.add(new TypeSafeMatcher<LogRecord>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format(
            "Expected log message of level [%s] containg message [%s] with exception [%s] "
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
    });
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
    Collection<Matcher<LogRecord>> missingExpecations = Lists.newArrayList();
    FOUND: for (Matcher<LogRecord> expectation : expectations) {
      for (LogRecord log : logSaver.getLogs()) {
        if (expectation.matches(log)) {
          continue FOUND;
        }
      }
      missingExpecations.add(expectation);
    }

    if (!missingExpecations.isEmpty()) {
      fail(String.format("Missed logging expectations: %s", missingExpecations));
    }
  }

  private final Logger log;
  private final LogSaver logSaver;
  private final Collection<Matcher<LogRecord>> expectations;
  private Level previousLevel;

  private ExpectedLogs(Class<?> klass) {
    log = Logger.getLogger(klass.getName());
    logSaver = new LogSaver();
    expectations = Lists.newArrayList();
  }

  /**
   * A JUL logging {@link Handler} that records all logging events which are passed to it.
   */
  @ThreadSafe
  private static class LogSaver extends Handler {
    Collection<LogRecord> logRecords = new ConcurrentLinkedDeque<>();

    public Collection<LogRecord> getLogs() {
      return logRecords;
    }

    @Override
    public void publish(LogRecord record) {
      logRecords.add(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}
  }
}
