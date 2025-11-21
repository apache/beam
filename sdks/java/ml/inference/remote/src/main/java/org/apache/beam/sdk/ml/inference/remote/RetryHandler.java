/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.ml.inference.remote;

import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 *  A utility for running request and handle failures and retries.
 */
public class RetryHandler implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(RetryHandler.class);

  private final int maxRetries;
  private final Duration initialBackoff;
  private final Duration maxBackoff;
  private final Duration maxCumulativeBackoff;

  private RetryHandler(
    int maxRetries,
    Duration initialBackoff,
    Duration maxBackoff,
    Duration maxCumulativeBackoff) {
    this.maxRetries = maxRetries;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
    this.maxCumulativeBackoff = maxCumulativeBackoff;
  }

  public static RetryHandler withDefaults() {
    return new RetryHandler(
      3, // maxRetries
      Duration.standardSeconds(1), // initialBackoff
      Duration.standardSeconds(10), // maxBackoff per retry
      Duration.standardMinutes(1) // maxCumulativeBackoff
    );
  }

  public <T> T execute(RetryableRequest<T> request) throws Exception {
    BackOff backoff = FluentBackoff.DEFAULT
      .withMaxRetries(maxRetries)
      .withInitialBackoff(initialBackoff)
      .withMaxBackoff(maxBackoff)
      .withMaxCumulativeBackoff(maxCumulativeBackoff)
      .backoff();

    Sleeper sleeper = Sleeper.DEFAULT;
    Exception lastException;
    int attempt = 0;

    while (true) {
      try {
        return request.call();

      } catch (Exception e) {
        lastException = e;

        long backoffMillis = backoff.nextBackOffMillis();

        if (backoffMillis == BackOff.STOP) {
          LOG.error("Request failed after {} retry attempts.", attempt);
          throw new RuntimeException(
            "Request failed after exhausting retries. " +
              "Max retries: " + maxRetries + ", " ,
            lastException);
        }

        attempt++;
        LOG.warn("Retry request attempt {} failed with: {}. Retrying in {} ms", attempt, e.getMessage(), backoffMillis);

        sleeper.sleep(backoffMillis);
      }
    }
  }

  @FunctionalInterface
  public interface RetryableRequest<T> {

    T call() throws Exception;
  }
}
