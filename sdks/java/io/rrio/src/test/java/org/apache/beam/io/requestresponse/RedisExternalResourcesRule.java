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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.net.URI;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.rules.ExternalResource;
import redis.clients.jedis.JedisPooled;

/**
 * {@link org.junit.runners.JUnit4} {@link org.junit.Rule} for {@link JedisPooled} based clients.
 */
class RedisExternalResourcesRule extends ExternalResource {

  private final Supplier<URI> configurationSupplier;

  private @MonotonicNonNull JedisPooled validatingClient;
  private @MonotonicNonNull RedisClient actualClient;

  RedisExternalResourcesRule(Supplier<URI> configurationSupplier) {
    this.configurationSupplier = configurationSupplier;
  }

  @Override
  protected void before() throws Throwable {
    URI uri = configurationSupplier.get();
    validatingClient = new JedisPooled(uri);
    actualClient = new RedisClient(uri);

    validatingClient.ping();
    actualClient.setup();
  }

  @Override
  protected void after() {
    getValidatingClient().close();
    try {
      getActualClient().teardown();
    } catch (UserCodeExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @NonNull
  JedisPooled getValidatingClient() {
    return checkStateNotNull(validatingClient);
  }

  public RedisClient getActualClient() {
    return checkStateNotNull(actualClient);
  }
}
