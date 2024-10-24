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
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.transforms.DoFn;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisException;

/**
 * {@link RedisClient} is a convenience class that supports operations needed for caching by various
 * transforms in this package. It implements the {@link SetupTeardown} interface for ease-of-use
 * within a {@link DoFn} context. Unlike the underlying {@link JedisPooled} client, {@link
 * RedisClient} is {@link java.io.Serializable}.
 */
class RedisClient implements SetupTeardown {

  private final URI uri;

  private transient @MonotonicNonNull JedisPooled jedis;

  /**
   * Instantiates a {@link RedisClient}. {@link URI} expected of the form: {@code
   * redis://<host>:<port>}.
   */
  RedisClient(URI uri) {
    this.uri = uri;
  }

  /**
   * Decrement a value stored by the key, returning the resulting decremented value. Per Redis
   * convention, sets the value to -1 for keys that do not exist. Naming of this method preserves
   * that of the underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  long decr(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().decr(key);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /** Get a byte array associated with a byte array key. Returns null if key does not exist. */
  byte @Nullable [] getBytes(byte[] key) throws UserCodeExecutionException {
    try {
      return getSafeClient().get(key);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Get the long value stored by the key. Yields zero when key does not exist, keeping consistency
   * with Redis convention. Consider using {@link #exists} to query key existance.
   */
  long getLong(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().decrBy(key, 0L);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /** Query whether the key exists. */
  boolean exists(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().exists(key);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Increment a value stored by the key, returning the resulting decremented value. Sets the value
   * to 1, if key does not exist, per Redis convention. Naming of this method preserves that of the
   * underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  long incr(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().incr(key);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Query the size of a list identified by the key. Returns 0 if key does not exist, per Redis
   * convention. Naming of this method preserves that of the underlying {@link JedisPooled} client
   * and performs a null check prior to execution.
   */
  long llen(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().llen(key);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /** Query whether the Redis list is empty. Calls {@link #llen} to determine this. */
  boolean isEmpty(String key) throws UserCodeExecutionException {
    return this.llen(key) == 0L;
  }

  /**
   * Pushes items to the back ('right') of the list. Naming of this method preserves that of the
   * underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  void rpush(String key, byte[]... items) throws UserCodeExecutionException {
    try {
      getSafeClient().rpush(key.getBytes(StandardCharsets.UTF_8), items);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Pops items from the front ('left') of the list. Naming of this method preserves that of the
   * underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  byte[] lpop(String key) throws UserCodeExecutionException {
    try {
      return getSafeClient().lpop(key.getBytes(StandardCharsets.UTF_8));
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Sets the key/value for a Duration expiry. Naming of this method preserves that of the
   * underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  void setex(byte[] key, byte[] value, Duration expiry) throws UserCodeExecutionException {
    try {
      getSafeClient().setex(key, expiry.getStandardSeconds(), value);
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /**
   * Sets the key/value for a Duration expiry. Naming of this method preserves that of the
   * underlying {@link JedisPooled} client and performs a null check prior to execution.
   */
  void setex(String key, Long value, Duration expiry) throws UserCodeExecutionException {
    try {
      getSafeClient().setex(key, expiry.getStandardSeconds(), String.valueOf(value));
    } catch (JedisException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /** Overrides {@link SetupTeardown}'s {@link SetupTeardown#setup} method. */
  @Override
  public void setup() throws UserCodeExecutionException {
    try {
      jedis = new JedisPooled(uri);
      jedis.ping();
    } catch (JedisException e) {
      String message =
          String.format("Failed to connect to host: %s, error: %s", uri, e.getMessage());
      throw new UserCodeExecutionException(message, e);
    }
  }

  private @NonNull JedisPooled getSafeClient() {
    return checkStateNotNull(jedis);
  }

  /** Overrides {@link SetupTeardown}'s {@link SetupTeardown#teardown} method. */
  @Override
  public void teardown() throws UserCodeExecutionException {
    if (jedis != null) {
      jedis.close();
    }
  }
}
