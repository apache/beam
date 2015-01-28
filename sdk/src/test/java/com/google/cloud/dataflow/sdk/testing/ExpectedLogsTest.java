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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

/** Tests for {@link FastNanoClockAndSleeper}. */
@RunWith(JUnit4.class)
public class ExpectedLogsTest {
  private static final Logger LOG = LoggerFactory.getLogger(ExpectedLogsTest.class);

  private ExpectedLogs expectedLogs;

  @Before
  public void setUp() {
    expectedLogs = ExpectedLogs.none(ExpectedLogsTest.class);
  }

  @Test
  public void testWhenNoExpectations() throws Throwable {
    expectedLogs.before();
    LOG.error(generateRandomString());
    expectedLogs.after();
  }

  @Test
  public void testWhenExpectationIsMatchedFully() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.before();
    expectedLogs.expectError(expected);
    LOG.error(expected);
    expectedLogs.after();
  }

  @Test
  public void testWhenExpectationIsMatchedPartially() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.before();
    expectedLogs.expectError(expected);
    LOG.error("Extra stuff around expected " + expected + " blah");
    expectedLogs.after();
  }

  @Test
  public void testWhenExpectationIsMatchedWithExceptionBeingLogged() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.before();
    expectedLogs.expectError(expected);
    LOG.error(expected, new IOException("Fake Exception"));
    expectedLogs.after();
  }

  @Test(expected = AssertionError.class)
  public void testWhenExpectationIsNotMatched() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.before();
    expectedLogs.expectError(expected);
    expectedLogs.after();
  }

  @Test
  public void testLogCaptureOccursAtLowestLogLevel() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.before();
    expectedLogs.expectTrace(expected);
    LOG.trace(expected);
    expectedLogs.after();
  }

  @Test
  public void testThreadSafetyOfLogSaver() throws Throwable {
    expectedLogs.before();

    CompletionService<Void> completionService =
        new ExecutorCompletionService<>(Executors.newCachedThreadPool());
    final long scheduledLogTime = System.currentTimeMillis() + 500L;
    for (int i = 0; i < 100; i++) {
      final String expected = generateRandomString();
      expectedLogs.expectTrace(expected);
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // Have all threads started and waiting to log at about the same moment.
          Thread.sleep(Math.max(1, scheduledLogTime - System.currentTimeMillis()));
          LOG.trace(expected);
          return null;
        }
      });
    }

    // Wait for all the threads to complete.
    for (int i = 0; i < 100; i++) {
      completionService.take();
    }

    expectedLogs.after();
  }

  // Generates a random fake error message.
  private static String generateRandomString() {
    Random random = new Random();
    return "Fake error message: " + random.nextInt();
  }
}
