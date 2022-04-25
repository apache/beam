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
package org.apache.beam.sdk.io.aws2.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.checkerframework.dataflow.qual.Pure;

/**
 * HTTP client configuration for both, sync and async AWS clients.
 *
 * <p>All timeouts are configured in milliseconds.
 *
 * @see <a
 *     href="https://github.com/aws/aws-sdk-java-v2/blob/master/docs/LaunchChangelog.md#131-client-http-configuration">Client
 *     HTTP Configuration</a>
 */
@AutoValue
@JsonInclude(value = JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = HttpClientConfiguration.Builder.class)
public abstract class HttpClientConfiguration implements Serializable {

  /**
   * Milliseconds to wait when acquiring a connection from the pool before giving up and timing out.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer connectionAcquisitionTimeout();

  /**
   * Maximum milliseconds a connection should be allowed to remain open while idle.
   *
   * <p>This will never close a connection that is currently in use, so long-lived connections may
   * remain open longer than this time.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer connectionMaxIdleTime();

  /**
   * Milliseconds to wait when initially establishing a connection before giving up and timing out.
   * A duration of 0 means infinity, and is not recommended.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer connectionTimeout();

  /**
   * Maximum milliseconds a connection should be allowed to remain open, regardless of usage
   * frequency.
   *
   * <p>This will never close a connection that is currently in use, so long-lived connections may
   * remain open longer than this time.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer connectionTimeToLive();

  /**
   * Milliseconds to wait for data to be transferred over an established, open connection before the
   * connection is timed out. A duration of 0 means infinity, and is not recommended.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer socketTimeout();

  /**
   * Milliseconds to wait for a read on a socket before an exception is thrown. A duration of 0
   * means infinity, and is not recommended.
   *
   * <p>Note: Read timeout is only supported for async clients and ignored otherwise. Use {@link
   * #socketTimeout()} instead.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer readTimeout();

  /**
   * Milliseconds to wait for a write on a socket before an exception is thrown. A duration of 0
   * means infinity, and is not recommended.
   *
   * <p>Note: Write timeout is only supported for async clients and ignored otherwise. Use {@link
   * #socketTimeout()} instead.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer writeTimeout();

  /**
   * The maximum number of connections allowed in the connection pool. Each client has its own
   * private connection pool.
   *
   * <p>For asynchronous clients using HTTP/1.1 this corresponds to the maximum number of allowed
   * concurrent requests. When using HTTP/2 the number of connections that will be used depends on
   * the max streams allowed per connection.
   */
  @JsonProperty
  public abstract @Nullable @Pure Integer maxConnections();

  public static Builder builder() {
    return Builder.builder();
  }

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "")
  public abstract static class Builder {
    @JsonCreator
    static Builder builder() {
      return new AutoValue_HttpClientConfiguration.Builder();
    }

    /**
     * Milliseconds to wait when acquiring a connection from the pool before giving up and timing
     * out.
     */
    public abstract Builder connectionAcquisitionTimeout(Integer millis);

    /**
     * Maximum milliseconds a connection should be allowed to remain open while idle.
     *
     * <p>This will never close a connection that is currently in use, so long-lived connections may
     * remain open longer than this time.
     */
    public abstract Builder connectionMaxIdleTime(Integer millis);

    /**
     * Milliseconds to wait when initially establishing a connection before giving up and timing
     * out. A duration of 0 means infinity, and is not recommended.
     */
    public abstract Builder connectionTimeout(Integer millis);

    /**
     * Maximum milliseconds a connection should be allowed to remain open, regardless of usage
     * frequency.
     *
     * <p>This will never close a connection that is currently in use, so long-lived connections may
     * remain open longer than this time.
     */
    public abstract Builder connectionTimeToLive(Integer millis);

    /**
     * Milliseconds to wait for data to be transferred over an established, open connection before
     * the connection is timed out. A duration of 0 means infinity, and is not recommended.
     */
    public abstract Builder socketTimeout(Integer millis);

    /**
     * Milliseconds to wait for a read on a socket before an exception is thrown. A duration of 0
     * means infinity, and is not recommended.
     *
     * <p>Note: Read timeout is only supported for async clients and ignored otherwise, set {@link
     * #socketTimeout(Integer)} ()} instead.
     */
    public abstract Builder readTimeout(Integer millis);

    /**
     * Milliseconds to wait for a write on a socket before an exception is thrown. A duration of 0
     * means infinity, and is not recommended.
     *
     * <p>Note: Write timeout is only supported for async clients and ignored otherwise, set {@link
     * #socketTimeout(Integer)} ()} instead.
     */
    public abstract Builder writeTimeout(Integer millis);

    /**
     * The maximum number of connections allowed in the connection pool. Each client has its own
     * private connection pool.
     *
     * <p>For asynchronous clients using HTTP/1.1 this corresponds to the maximum number of allowed
     * concurrent requests. When using HTTP/2 the number of connections that will be used depends on
     * the max streams allowed per connection.
     */
    public abstract Builder maxConnections(Integer connections);

    public abstract HttpClientConfiguration build();
  }
}
