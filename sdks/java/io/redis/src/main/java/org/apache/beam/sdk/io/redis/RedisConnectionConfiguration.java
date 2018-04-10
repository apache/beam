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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.display.DisplayData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

/**
 * {@code RedisConnectionConfiguration} describes and wraps a connectionConfiguration to Redis
 * server or cluster.
 */
@AutoValue
public abstract class RedisConnectionConfiguration implements Serializable {

  abstract String host();
  abstract int port();
  @Nullable abstract String auth();
  abstract int timeout();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setHost(String host);
    abstract Builder setPort(int port);
    abstract Builder setAuth(String auth);
    abstract Builder setTimeout(int timeout);
    abstract RedisConnectionConfiguration build();
  }

  public static RedisConnectionConfiguration create() {
    return new AutoValue_RedisConnectionConfiguration.Builder()
        .setHost(Protocol.DEFAULT_HOST)
        .setPort(Protocol.DEFAULT_PORT)
        .setTimeout(Protocol.DEFAULT_TIMEOUT).build();
  }

  public static RedisConnectionConfiguration create(String host, int port) {
    return new AutoValue_RedisConnectionConfiguration.Builder()
        .setHost(host)
        .setPort(port)
        .setTimeout(Protocol.DEFAULT_TIMEOUT).build();
  }

  /**
   * Define the host name of the Redis server.
   */
  public RedisConnectionConfiguration withHost(String host) {
    checkArgument(host != null, "host can not be null");
    return builder().setHost(host).build();
  }

  /**
   * Define the port number of the Redis server.
   */
  public RedisConnectionConfiguration withPort(int port) {
    checkArgument(port > 0, "port can not be negative or 0");
    return builder().setPort(port).build();
  }

  /**
   * Define the password to authenticate on the Redis server.
   */
  public RedisConnectionConfiguration withAuth(String auth) {
    checkArgument(auth != null, "auth can not be null");
    return builder().setAuth(auth).build();
  }

  /**
   * Define the Redis connection timeout. A timeout of zero is interpreted as an infinite timeout.
   */
  public RedisConnectionConfiguration withTimeout(int timeout) {
    checkArgument(timeout >= 0, "timeout can not be negative");
    return builder().setTimeout(timeout).build();
  }

  /**
   * Connect to the Redis instance.
   */
  public Jedis connect() {
    Jedis jedis = new Jedis(host(), port(), timeout());
    if (auth() != null) {
      jedis.auth(auth());
    }
    return jedis;
  }

  /**
   * Populate the display data with connectionConfiguration details.
   */
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("host", host()));
    builder.add(DisplayData.item("port", port()));
    builder.addIfNotNull(DisplayData.item("timeout", timeout()));
  }

}
