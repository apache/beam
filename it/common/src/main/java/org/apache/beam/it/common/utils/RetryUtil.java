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
package org.apache.beam.it.common.utils;

import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;

/** Utility Class related to retry operations. */
public class RetryUtil {

  public static final int DEFAULT_BACKOFF_START_DELAY_MILLIS = 100;
  public static final int DEFAULT_BACKOFF_MAX_DELAY_MILLIS = 5000;
  public static final int DEFAULT_MAX_RETRIES = 3;

  private RetryUtil() {}

  /**
   * Create a policy to retry when client returns a "Server is not responding" or a transient error.
   * By default, it retries up to 3 times using a backoff strategy.
   *
   * @return Failsafe's RetryPolicy.
   */
  public static <T> RetryPolicy<T> clientRetryPolicy() {
    return RetryPolicy.<T>builder()
        .handle(IOException.class, SocketTimeoutException.class)
        .handleIf(
            throwable ->
                throwable.getMessage() != null
                    && throwable.getMessage().contains("Server is not responding"))
        .withBackoff(
            Duration.ofMillis(DEFAULT_BACKOFF_START_DELAY_MILLIS),
            Duration.ofMillis(DEFAULT_BACKOFF_MAX_DELAY_MILLIS))
        .withMaxRetries(DEFAULT_MAX_RETRIES)
        .build();
  }
}
