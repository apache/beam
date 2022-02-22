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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.checkerframework.dataflow.qual.Pure;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
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
public abstract class ClientConfiguration implements Serializable {

  /**
   * Optional {@link AwsCredentialsProvider}. If set, this overwrites the default in {@link
   * AwsOptions#getAwsCredentialsProvider()}.
   */
  public abstract @Nullable @Pure AwsCredentialsProvider credentialsProvider();

  /**
   * Optional {@link Region}. If set, this overwrites the default in {@link
   * AwsOptions#getAwsRegion()}.
   */
  public @Nullable @Pure Region region() {
    return regionId() != null ? Region.of(regionId()) : null;
  }

  /**
   * Optional service endpoint to use AWS compatible services instead, e.g. for testing. If set,
   * this overwrites the default in {@link AwsOptions#getEndpoint()}.
   */
  public abstract @Nullable @Pure URI endpoint();

  /**
   * Optional {@link RetryConfiguration} for AWS clients. If unset, retry behavior will be unchanged
   * and use SDK defaults.
   */
  public abstract @Nullable @Pure RetryConfiguration retry();

  abstract @Nullable @Pure String regionId();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_ClientConfiguration.Builder();
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
  public abstract static class Builder {
    /**
     * Optional {@link AwsCredentialsProvider}. If set, this overwrites the default in {@link
     * AwsOptions#getAwsCredentialsProvider()}.
     */
    public abstract Builder credentialsProvider(AwsCredentialsProvider credentialsProvider);

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

    abstract Builder regionId(String region);

    abstract AwsCredentialsProvider credentialsProvider();

    abstract ClientConfiguration autoBuild();

    public ClientConfiguration build() {
      if (credentialsProvider() != null) {
        credentialsProvider(new SerializableAwsCredentialsProvider(credentialsProvider()));
      }
      return autoBuild();
    }
  }

  /** Internal serializable {@link AwsCredentialsProvider}. */
  private static class SerializableAwsCredentialsProvider
      implements AwsCredentialsProvider, Serializable {
    private transient AwsCredentialsProvider provider;
    private String serializedProvider;

    SerializableAwsCredentialsProvider(AwsCredentialsProvider provider) {
      this.provider = checkNotNull(provider, "AwsCredentialsProvider cannot be null");
      this.serializedProvider = serializeAwsCredentialsProvider(provider);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      out.writeUTF(serializedProvider);
    }

    private void readObject(ObjectInputStream in) throws IOException {
      serializedProvider = in.readUTF();
      provider = deserializeAwsCredentialsProvider(serializedProvider);
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return provider.resolveCredentials();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SerializableAwsCredentialsProvider that = (SerializableAwsCredentialsProvider) o;
      return serializedProvider.equals(that.serializedProvider);
    }

    @Override
    public int hashCode() {
      return serializedProvider.hashCode();
    }
  }
}
