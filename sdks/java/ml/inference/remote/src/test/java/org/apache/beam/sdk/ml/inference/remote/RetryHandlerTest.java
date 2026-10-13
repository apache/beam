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
package org.apache.beam.sdk.ml.inference.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RetryHandlerTest {

  private static class NonRetryableException extends Exception {
    NonRetryableException(String message) {
      super(message);
    }
  }

  private static class RetryableException extends Exception {
    RetryableException(String message) {
      super(message);
    }
  }

  @Test
  public void testRetryWithDefaultFilter() throws Exception {
    RetryHandler handler = RetryHandler.withDefaults();
    AtomicInteger attempts = new AtomicInteger(0);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                handler.execute(
                    () -> {
                      attempts.incrementAndGet();
                      throw new Exception("Always fails");
                    }));

    assertTrue(thrown.getMessage().contains("exhausting retries"));
    assertEquals(4, attempts.get()); // 1 initial attempt + 3 retries
  }

  @Test
  public void testRetryWithCustomFilter_ShouldNotRetry() {
    RetryHandler handler =
        RetryHandler.withDefaults()
            .withRetryFilter(
                e -> {
                  if (e instanceof NonRetryableException) {
                    return false;
                  }
                  return true;
                });

    AtomicInteger attempts = new AtomicInteger(0);

    NonRetryableException thrown =
        assertThrows(
            NonRetryableException.class,
            () ->
                handler.execute(
                    () -> {
                      attempts.incrementAndGet();
                      throw new NonRetryableException("Should not retry");
                    }));

    assertEquals("Should not retry", thrown.getMessage());
    assertEquals(1, attempts.get()); // 1 initial attempt, no retries
  }

  @Test
  public void testRetryWithCustomFilter_ShouldRetry() {
    RetryHandler handler =
        RetryHandler.withDefaults()
            .withRetryFilter(
                e -> {
                  if (e instanceof NonRetryableException) {
                    return false;
                  }
                  return true;
                });

    AtomicInteger attempts = new AtomicInteger(0);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                handler.execute(
                    () -> {
                      attempts.incrementAndGet();
                      throw new RetryableException("Should retry");
                    }));

    assertTrue(thrown.getMessage().contains("exhausting retries"));
    assertEquals(4, attempts.get()); // 1 initial attempt + 3 retries
  }

  @Test
  public void testRetryWithCustomFilter_EventualSuccess() throws Exception {
    RetryHandler handler =
        RetryHandler.withDefaults()
            .withRetryFilter(
                e -> {
                  if (e instanceof NonRetryableException) {
                    return false;
                  }
                  return true;
                });

    AtomicInteger attempts = new AtomicInteger(0);

    String result =
        handler.execute(
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw new RetryableException("Temporary failure");
              }
              return "success";
            });

    assertEquals("success", result);
    assertEquals(3, attempts.get()); // 1 initial attempt + 2 retries
  }
}
