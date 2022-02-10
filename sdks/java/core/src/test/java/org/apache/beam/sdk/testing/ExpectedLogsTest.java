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

import static org.apache.beam.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogRecord;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link ExpectedLogs}. */
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

  @Test
  public void testVerifyLogRecords() throws Throwable {
    String expected = generateRandomString();
    LOG.error(expected);
    LOG.error(expected);
    expectedLogs.verifyLogRecords(
        new TypeSafeMatcher<Iterable<LogRecord>>() {
          @Override
          protected boolean matchesSafely(Iterable<LogRecord> item) {
            int count = 0;
            for (LogRecord record : item) {
              if (record.getMessage().contains(expected)) {
                count += 1;
              }
            }
            return count == 2;
          }

          @Override
          public void describeTo(org.hamcrest.Description description) {}
        });
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
      completionService.submit(
          () -> {
            // Have all threads started and waiting to log at about the same moment.
            sleepMillis(
                Math.max(
                    1,
                    scheduledLogTime
                        - TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS)));
            LOG.trace(expected);
            return null;
          });
    }

    // Wait for all the threads to complete.
    for (int i = 0; i < 100; i++) {
      completionService.take().get();
    }

    for (String expected : expectedStrings) {
      expectedLogs.verifyTrace(expected);
    }
  }

  @Test
  public void testLogsCleared() throws Throwable {
    final String messageUnexpected = "Message prior to ExpectedLogs.";
    final String messageExpected = "Message expected.";
    LOG.info(messageUnexpected);

    expectedLogs = ExpectedLogs.none(ExpectedLogsTest.class);
    final boolean[] evaluateRan = new boolean[1];

    expectedLogs
        .apply(
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                evaluateRan[0] = true;
                expectedLogs.verifyNotLogged(messageUnexpected);
                LOG.info(messageExpected);
                expectedLogs.verifyInfo(messageExpected);
              }
            },
            Description.EMPTY)
        .evaluate();
    assertTrue(evaluateRan[0]);
    // Verify expectedLogs is cleared.
    expectedLogs.verifyNotLogged(messageExpected);
    expectedLogs.verifyNotLogged(messageUnexpected);
  }

  // Generates a random fake error message.
  private String generateRandomString() {
    return "Fake error message: " + random.nextInt();
  }
}
