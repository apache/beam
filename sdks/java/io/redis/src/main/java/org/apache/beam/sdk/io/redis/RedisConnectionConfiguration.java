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
package org.apache.beam.sdk.io.redis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

/**
 * {@code RedisConnectionConfiguration} describes and wraps a connectionConfiguration to Redis
 * server or cluster.
 */
@AutoValue
public abstract class RedisConnectionConfiguration implements Serializable {

  abstract ValueProvider<String> host();

  abstract ValueProvider<Integer> port();

  abstract @Nullable ValueProvider<String> auth();

  abstract ValueProvider<Integer> timeout();

  abstract ValueProvider<Boolean> ssl();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setHost(ValueProvider<String> host);

    abstract Builder setPort(ValueProvider<Integer> port);

    abstract Builder setAuth(ValueProvider<String> auth);

    abstract Builder setTimeout(ValueProvider<Integer> timeout);

    abstract Builder setSsl(ValueProvider<Boolean> ssl);

    abstract RedisConnectionConfiguration build();
  }

  public static RedisConnectionConfiguration create() {
    return new AutoValue_RedisConnectionConfiguration.Builder()
        .setHost(ValueProvider.StaticValueProvider.of(Protocol.DEFAULT_HOST))
        .setPort(ValueProvider.StaticValueProvider.of(Protocol.DEFAULT_PORT))
        .setTimeout(ValueProvider.StaticValueProvider.of(Protocol.DEFAULT_TIMEOUT))
        .setSsl(ValueProvider.StaticValueProvider.of(Boolean.FALSE))
        .build();
  }

  public static RedisConnectionConfiguration create(String host, int port) {
    return create().withHost(host).withPort(port);
  }

  public static RedisConnectionConfiguration create(
      ValueProvider<String> host, ValueProvider<Integer> port) {
    return create().withHost(host).withPort(port);
  }

  /** Define the host name of the Redis server. */
  public RedisConnectionConfiguration withHost(String host) {
    checkArgument(host != null, "host can not be null");
    return withHost(ValueProvider.StaticValueProvider.of(host));
  }

  /** See {@link RedisConnectionConfiguration#withHost(String)}. */
  public RedisConnectionConfiguration withHost(ValueProvider<String> host) {
    return builder().setHost(host).build();
  }

  /** Define the port number of the Redis server. */
  public RedisConnectionConfiguration withPort(int port) {
    checkArgument(port > 0, "port can not be negative or 0");
    return withPort(ValueProvider.StaticValueProvider.of(port));
  }

  /** See {@link RedisConnectionConfiguration#withPort(int)}. */
  public RedisConnectionConfiguration withPort(ValueProvider<Integer> port) {
    return builder().setPort(port).build();
  }

  /** Define the password to authenticate on the Redis server. */
  public RedisConnectionConfiguration withAuth(String auth) {
    checkArgument(auth != null, "auth can not be null");
    return withAuth(ValueProvider.StaticValueProvider.of(auth));
  }

  /** See {@link RedisConnectionConfiguration#withAuth(String)}. */
  public RedisConnectionConfiguration withAuth(ValueProvider<String> auth) {
    return builder().setAuth(auth).build();
  }

  /**
   * Define the Redis connection timeout. A timeout of zero is interpreted as an infinite timeout.
   */
  public RedisConnectionConfiguration withTimeout(int timeout) {
    checkArgument(timeout >= 0, "timeout can not be negative");
    return withTimeout(ValueProvider.StaticValueProvider.of(timeout));
  }

  /** See {@link RedisConnectionConfiguration#withTimeout(int)}. */
  public RedisConnectionConfiguration withTimeout(ValueProvider<Integer> timeout) {
    return builder().setTimeout(timeout).build();
  }

  /** Enable SSL connection to Redis server. */
  public RedisConnectionConfiguration enableSSL() {
    return withSSL(ValueProvider.StaticValueProvider.of(Boolean.TRUE));
  }

  /** Define if a SSL connection to Redis server should be used. */
  public RedisConnectionConfiguration withSSL(ValueProvider<Boolean> ssl) {
    return builder().setSsl(ssl).build();
  }

  /** Connect to the Redis instance. */
  public Jedis connect() {
    Jedis jedis = new Jedis(host().get(), port().get(), timeout().get(), ssl().get());
    if (auth() != null) {
      jedis.auth(auth().get());
    }
    return jedis;
  }

  /** Populate the display data with connectionConfiguration details. */
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("host", host()));
    builder.add(DisplayData.item("port", port()));
    builder.addIfNotNull(DisplayData.item("timeout", timeout()));
    builder.add(DisplayData.item("ssl", ssl()));
  }
}
