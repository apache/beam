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

import static org.apache.beam.sdk.io.aws2.options.AwsSerializableUtils.deserializeAwsCredentialsProvider;
import static org.apache.beam.sdk.io.aws2.options.AwsSerializableUtils.serializeAwsCredentialsProvider;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import java.io.Serializable;
import java.net.URI;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.checkerframework.dataflow.qual.Pure;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * AWS client configuration.
 *
 * <p>AWS clients for all AWS IOs can be configured using {@link
 * org.apache.beam.sdk.io.aws2.options.AwsOptions}, which provides reasonable defaults based on
 * default providers of the AWS SDK.
 *
 * <p>{@link ClientConfiguration} is meant to override defaults for a specific IO, including {@link
 * RetryConfiguration}. Retries are handled by the AWS SDK unless there's partial success. The SDK
 * uses a backoff strategy with equal jitter for computing the delay before the next retry.
 */
@AutoValue
@JsonInclude(value = JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = ClientConfiguration.Builder.class)
public abstract class ClientConfiguration implements Serializable {
  public static final ClientConfiguration EMPTY = ClientConfiguration.builder().build();

  /**
   * Optional {@link AwsCredentialsProvider}. If set, this overwrites the default in {@link
   * AwsOptions#getAwsCredentialsProvider()}.
   */
  @JsonProperty
  @Memoized
  public @Nullable @Pure AwsCredentialsProvider credentialsProvider() {
    return credentialsProviderAsJson() != null
        ? deserializeAwsCredentialsProvider(credentialsProviderAsJson())
        : null;
  }

  /**
   * Optional {@link Region}. If set, this overwrites the default in {@link
   * AwsOptions#getAwsRegion()}.
   */
  @JsonProperty
  @Memoized
  public @Nullable @Pure Region region() {
    return regionId() != null ? Region.of(regionId()) : null;
  }

  /**
   * Optional flag to skip certificate verification. Should only be overriden for test scenarios. If
   * set, this overwrites the default in {@link AwsOptions#skipCertificateVerification()}.
   */
  @JsonProperty
  public abstract @Nullable @Pure Boolean skipCertificateVerification();

  /**
   * Optional service endpoint to use AWS compatible services instead, e.g. for testing. If set,
   * this overwrites the default in {@link AwsOptions#getEndpoint()}.
   */
  @JsonProperty
  public abstract @Nullable @Pure URI endpoint();

  /**
   * Optional {@link RetryConfiguration} for AWS clients. If unset, retry behavior will be unchanged
   * and use SDK defaults.
   */
  @JsonProperty
  public abstract @Nullable @Pure RetryConfiguration retry();

  abstract @Nullable @Pure String regionId();

  abstract @Nullable @Pure String credentialsProviderAsJson();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return Builder.builder();
  }

  public static ClientConfiguration create(
      AwsCredentialsProvider credentials, Region region, @Nullable URI endpoint) {
    Builder builder = ClientConfiguration.builder().credentialsProvider(credentials).region(region);
    if (endpoint != null) {
      builder.endpoint(endpoint);
    }
    return builder.build();
  }

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "")
  public abstract static class Builder {
    @JsonCreator
    static Builder builder() {
      return new AutoValue_ClientConfiguration.Builder();
    }

    /**
     * Optional {@link AwsCredentialsProvider}. If set, this overwrites the default in {@link
     * AwsOptions#getAwsCredentialsProvider()}.
     */
    public Builder credentialsProvider(AwsCredentialsProvider credentialsProvider) {
      return credentialsProviderAsJson(serializeAwsCredentialsProvider(credentialsProvider));
    }

    /**
     * Optional {@link Region}. If set, this overwrites the default in {@link
     * AwsOptions#getAwsRegion()}.
     */
    public Builder region(Region region) {
      return regionId(region.id()); // Region is not serializable, keep id internally
    }

    /**
     * Optional service endpoint to use AWS compatible services instead, e.g. for testing. If set,
     * this overwrites the default in {@link AwsOptions#getEndpoint()}.
     */
    public abstract Builder endpoint(URI uri);

    /**
     * Optional {@link RetryConfiguration} for AWS clients. If unset, retry behavior will be
     * unchanged and use SDK defaults.
     */
    @JsonSetter
    public abstract Builder retry(RetryConfiguration retry);

    /**
     * Optional {@link RetryConfiguration} for AWS clients. If unset, retry behavior will be
     * unchanged and use SDK defaults.
     */
    public Builder retry(Consumer<RetryConfiguration.Builder> retry) {
      RetryConfiguration.Builder builder = RetryConfiguration.builder();
      retry.accept(builder);
      return retry(builder.build());
    }

    /**
     * Optional flag to skip certificate verification. Should only be overriden for test scenarios.
     * If set, this overwrites the default in {@link AwsOptions#skipCertificateVerification()}.
     */
    @JsonProperty
    public abstract Builder skipCertificateVerification(boolean skipCertificateVerification);

    abstract Builder regionId(String region);

    abstract Builder credentialsProviderAsJson(String credentialsProvider);

    public abstract ClientConfiguration build();
  }
}
