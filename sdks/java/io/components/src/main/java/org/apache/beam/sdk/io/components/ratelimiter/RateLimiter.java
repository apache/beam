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
package org.apache.beam.sdk.io.components.ratelimiter;

import java.io.IOException;
import java.io.Serializable;

/**
 * A RateLimiter allows to fetch permits from a rate limiter service and blocks execution when the
 * rate limit is exceeded.
 *
 * <p>Implementations must be {@link Serializable} as they are passed to workers.
 */
public interface RateLimiter extends Serializable, AutoCloseable {

  /**
   * Blocks until the specified number of permits are acquired and returns true if the request was
   * allowed or false if the request was rejected.
   *
   * @param permits Number of permits to acquire.
   * @return true if the request was allowed, false if it was rejected (and retries exceeded).
   * @throws IOException if there is an error communicating with the rate limiter service.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  boolean allow(int permits) throws IOException, InterruptedException;
}