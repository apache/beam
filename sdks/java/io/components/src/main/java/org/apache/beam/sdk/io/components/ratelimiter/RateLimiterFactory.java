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
 * A factory that manages connections to rate limit service and creates lightweight handles.
 *
 * <p>Implementations must be {@link Serializable} as they are passed to workers. The factory
 * typically manages the heavy connection (e.g. gRPC stub) and is thread-safe.
 */
public interface RateLimiterFactory extends Serializable, AutoCloseable {

  /**
   * Creates a lightweight ratelimiter handle bound to a specific context.
   *
   * <p>Use this when passing ratelimiter to IO components, which doesn't need to know about the
   * configuration or the underlying ratelimiter service details. This is also useful in DoFns when
   * you want to use the ratelimiter in a static way based on the compile time context.
   *
   * @param context The context for the ratelimit.
   * @return A {@link RateLimiter} handle.
   */
  RateLimiter getLimiter(RateLimiterContext context);

  /**
   * Blocks until the specified number of permits are acquired and returns true if the request was
   * allowed or false if the request was rejected.
   *
   * <p>Use this for when the ratelimit namespace or descriptors are not known at compile time.
   * allows you to use the ratelimiter in a dynamic way based on the runtime data.
   *
   * @param context The context for the ratelimit.
   * @param permits Number of permits to acquire.
   * @return true if the request is allowed, false if rejected.
   * @throws IOException if there is an error communicating with the ratelimiter service.
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  boolean allow(RateLimiterContext context, int permits) throws IOException, InterruptedException;
}
