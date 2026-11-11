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
package org.apache.beam.sdk.io.aws2.auth;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.util.Secret;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * An {@link AwsCredentialsProvider} that resolves static AWS credentials through a secret manager
 * (e.g. {@code GoogleCloudSecretManager}), so that the credentials are never passed or serialized
 * as plain text.
 *
 * <p>The access key ID, secret access key and optional session token are secret specs, parsed with
 * {@link Secret#fromJson(String, String)} using the configured secret manager. Only the specs are
 * serialized (e.g. in pipeline options); the credentials are fetched lazily when they are first
 * needed, typically on the workers.
 *
 * <p>Fetched values are cached per JVM to avoid calling the secret manager for every AWS client
 * that is created. Note that a rotated secret is only picked up by new worker processes.
 *
 * <p>AWS Secrets Manager cannot be used as the secret manager, because the AWS credentials are
 * needed to access it.
 *
 * <p>This provider is usually configured with the {@code --awsAccessKeyId}, {@code
 * --awsSecretAccessKey}, {@code --awsSessionToken} and {@code --awsCredentialsSecretManager}
 * pipeline options of {@link org.apache.beam.sdk.io.aws2.options.AwsOptions}, or directly:
 *
 * <pre>{@code --awsCredentialsProvider={
 *   "@type": "SecretCredentialsProvider",
 *   "secretManager": "GoogleCloudSecretManager",
 *   "accessKeyId": {"name": "my-aws-key-id", "project": "my-project"},
 *   "secretAccessKey": {"name": "my-aws-secret-key", "project": "my-project"}
 * }}</pre>
 */
public final class SecretCredentialsProvider implements AwsCredentialsProvider, Serializable {
  private static final long serialVersionUID = 1L;

  /** Secret manager names (lower case) that resolve to AWS Secrets Manager. */
  private static final ImmutableSet<String> AWS_SECRET_MANAGERS =
      ImmutableSet.of("awssecretsmanager", "awssecret");

  // Cache fetched secrets per JVM, keyed by (secret manager, spec). AWS clients are created
  // frequently (e.g. for most S3 filesystem operations), so this avoids calling the secret manager
  // each time.
  private static final Cache<List<String>, String> SECRET_CACHE =
      CacheBuilder.newBuilder().maximumSize(32).build();

  private final String secretManager;
  private final String accessKeyId;
  private final String secretAccessKey;
  private final @Nullable String sessionToken;

  private transient volatile @Nullable AwsCredentials credentials;

  private SecretCredentialsProvider(
      String secretManager,
      String accessKeyId,
      String secretAccessKey,
      @Nullable String sessionToken) {
    this.secretManager = secretManager;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
  }

  /**
   * Creates a provider for basic credentials.
   *
   * @param secretManager The secret manager used to parse the specs, e.g. {@code
   *     GoogleCloudSecretManager}.
   * @param accessKeyId The secret spec of the AWS access key ID.
   * @param secretAccessKey The secret spec of the AWS secret access key.
   */
  public static SecretCredentialsProvider create(
      String secretManager, String accessKeyId, String secretAccessKey) {
    return create(secretManager, accessKeyId, secretAccessKey, null);
  }

  /**
   * Creates a provider for basic or, if {@code sessionToken} is set, session credentials.
   *
   * @param secretManager The secret manager used to parse the specs, e.g. {@code
   *     GoogleCloudSecretManager}.
   * @param accessKeyId The secret spec of the AWS access key ID.
   * @param secretAccessKey The secret spec of the AWS secret access key.
   * @param sessionToken The optional secret spec of the AWS session token.
   */
  public static SecretCredentialsProvider create(
      String secretManager,
      String accessKeyId,
      String secretAccessKey,
      @Nullable String sessionToken) {
    checkArgument(!Strings.isNullOrEmpty(secretManager), "secretManager is required");
    checkArgument(!Strings.isNullOrEmpty(accessKeyId), "accessKeyId is required");
    checkArgument(!Strings.isNullOrEmpty(secretAccessKey), "secretAccessKey is required");
    String trimmedSecretManager = secretManager.trim();
    checkArgument(
        !AWS_SECRET_MANAGERS.contains(trimmedSecretManager.toLowerCase(Locale.ROOT)),
        "The AWS credentials secret manager cannot be AWS Secrets Manager, because the AWS "
            + "credentials are needed to access it. Store the credentials in another secret "
            + "manager, e.g. GoogleCloudSecretManager.");
    return new SecretCredentialsProvider(
        trimmedSecretManager,
        accessKeyId,
        secretAccessKey,
        Strings.isNullOrEmpty(sessionToken) ? null : sessionToken);
  }

  /** The secret manager used to parse the specs. */
  public String secretManager() {
    return secretManager;
  }

  /** The secret spec of the AWS access key ID. */
  public String accessKeyId() {
    return accessKeyId;
  }

  /** The secret spec of the AWS secret access key. */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  /** The optional secret spec of the AWS session token. */
  public @Nullable String sessionToken() {
    return sessionToken;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    AwsCredentials result = credentials;
    if (result == null) {
      synchronized (this) {
        result = credentials;
        if (result == null) {
          String resolvedKeyId = fetch(accessKeyId);
          String resolvedSecret = fetch(secretAccessKey);
          String localSessionToken = sessionToken;
          result =
              localSessionToken == null
                  ? AwsBasicCredentials.create(resolvedKeyId, resolvedSecret)
                  : AwsSessionCredentials.create(
                      resolvedKeyId, resolvedSecret, fetch(localSessionToken));
          credentials = result;
        }
      }
    }
    return result;
  }

  private String fetch(String spec) {
    try {
      return SECRET_CACHE.get(
          Arrays.asList(secretManager, spec),
          () -> {
            String value = Secret.fromJson(spec, secretManager).getString();
            if (value == null) {
              throw new IllegalStateException(
                  "Secret manager " + secretManager + " returned no value for an AWS credential");
            }
            return value;
          });
    } catch (ExecutionException | UncheckedExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      // Do not include the spec or value in the message, only the secret manager.
      throw new IllegalStateException(
          "Failed to resolve AWS credentials through secret manager " + secretManager, cause);
    }
  }

  @VisibleForTesting
  static void clearCache() {
    SECRET_CACHE.invalidateAll();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretCredentialsProvider)) {
      return false;
    }
    SecretCredentialsProvider that = (SecretCredentialsProvider) o;
    return secretManager.equals(that.secretManager)
        && accessKeyId.equals(that.accessKeyId)
        && secretAccessKey.equals(that.secretAccessKey)
        && Objects.equals(sessionToken, that.sessionToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(secretManager, accessKeyId, secretAccessKey, sessionToken);
  }

  @Override
  public String toString() {
    // Specs identify secrets but are not secret themselves; resolved values are never included.
    return "SecretCredentialsProvider{secretManager=" + secretManager + "}";
  }
}
