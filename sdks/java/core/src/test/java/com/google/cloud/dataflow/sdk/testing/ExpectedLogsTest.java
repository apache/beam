/*
 * Copyright (C) 2015 Google Inc.
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

import static com.google.cloud.dataflow.sdk.testing.SystemNanoTimeSleeper.sleepMillis;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Tests for {@link FastNanoClockAndSleeper}. */
@RunWith(JUnit4.class)
public class ExpectedLogsTest {
  private static final Logger LOG = LoggerFactory.getLogger(ExpectedLogsTest.class);

  private Random random = new Random();

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(ExpectedLogsTest.class);

  @Test
  public void testWhenNoExpectations() throws Throwable {
    LOG.error(generateRandomString());
  }

  @Test
  public void testVerifyWhenMatchedFully() throws Throwable {
    String expected = generateRandomString();

    LOG.error(expected);
    expectedLogs.verifyError(expected);
  }

  @Test
  public void testVerifyWhenMatchedPartially() throws Throwable {
    String expected = generateRandomString();
    LOG.error("Extra stuff around expected " + expected + " blah");
    expectedLogs.verifyError(expected);
  }

  @Test
  public void testVerifyWhenMatchedWithExceptionBeingLogged() throws Throwable {
    String expected = generateRandomString();
    LOG.error(expected, new IOException("Fake Exception"));
    expectedLogs.verifyError(expected);
  }

  @Test(expected = AssertionError.class)
  public void testVerifyWhenNotMatched() throws Throwable {
    String expected = generateRandomString();

    expectedLogs.verifyError(expected);
  }

  @Test(expected = AssertionError.class)
  public void testVerifyNotLoggedWhenMatchedFully() throws Throwable {
    String expected = generateRandomString();

    LOG.error(expected);
    expectedLogs.verifyNotLogged(expected);
  }

  @Test(expected = AssertionError.class)
  public void testVerifyNotLoggedWhenMatchedPartially() throws Throwable {
    String expected = generateRandomString();
    LOG.error("Extra stuff around expected " + expected + " blah");
    expectedLogs.verifyNotLogged(expected);
  }

  @Test(expected = AssertionError.class)
  public void testVerifyNotLoggedWhenMatchedWithException() throws Throwable {
    String expected = generateRandomString();
    LOG.error(expected, new IOException("Fake Exception"));
    expectedLogs.verifyNotLogged(expected);
  }

  @Test
  public void testVerifyNotLoggedWhenNotMatched() throws Throwable {
    String expected = generateRandomString();
    expectedLogs.verifyNotLogged(expected);
  }

  @Test
  public void testLogCaptureOccursAtLowestLogLevel() throws Throwable {
    String expected = generateRandomString();
    LOG.trace(expected);
    expectedLogs.verifyTrace(expected);
  }

  @Test
  public void testThreadSafetyOfLogSaver() throws Throwable {
    CompletionService<Void> completionService =
        new ExecutorCompletionService<>(Executors.newCachedThreadPool());
    final long scheduledLogTime =
        TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS) + 500L;

    List<String> expectedStrings = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final String expected = generateRandomString();
      expectedStrings.add(expected);
      completionService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // Have all threads started and waiting to log at about the same moment.
          sleepMillis(Math.max(1, scheduledLogTime
              - TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS)));
          LOG.trace(expected);
          return null;
        }
      });
    }

    // Wait for all the threads to complete.
    for (int i = 0; i < 100; i++) {
      completionService.take();
    }

    for (String expected : expectedStrings) {
      expectedLogs.verifyTrace(expected);
    }
  }

  // Generates a random fake error message.
  private String generateRandomString() {
    return "Fake error message: " + random.nextInt();
  }
}
