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

/**
 * A lightweight handle for an Envoy-based rate limiter.
 *
 * <p>Delegates work to the {@link EnvoyRateLimiterFactory} using the baked-in {@link
 * EnvoyRateLimiterContext}.
 */
public class EnvoyRateLimiter implements RateLimiter {
  private final EnvoyRateLimiterFactory factory;
  private final EnvoyRateLimiterContext context;

  public EnvoyRateLimiter(EnvoyRateLimiterFactory factory, EnvoyRateLimiterContext context) {
    this.factory = factory;
    this.context = context;
  }

  @Override
  public boolean allow(int permits) throws IOException, InterruptedException {
    return factory.allow(context, permits);
  }

  @Override
  public void close() throws Exception {
    factory.close();
  }
}
