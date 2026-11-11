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
package org.apache.beam.sdk.io.aws2.options;

import java.net.URI;
import java.util.Objects;
import org.apache.beam.sdk.io.aws2.auth.SecretCredentialsProvider;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Resolves the effective AWS client settings from {@link AwsOptions}.
 *
 * <p>{@link AwsOptions} has deprecated options ({@code --awsRegion}, {@code --endpoint}) and an
 * explicit {@code --awsCredentialsProvider} that overlap with newer options ({@code
 * --awsRegionName}, {@code --awsEndpointUrl}, {@code --awsAccessKeyId}, ...). The methods of this
 * class return the effective setting and fail if overlapping options are set to conflicting values.
 */
public final class AwsOptionsResolver {

  private AwsOptionsResolver() {}

  /**
   * Returns the effective region: {@code --awsRegionName} if set, otherwise the deprecated {@code
   * --awsRegion} (which defaults to the AWS default region provider chain).
   *
   * @throws IllegalArgumentException if {@code --awsRegion} and {@code --awsRegionName} are set to
   *     different regions.
   */
  @SuppressWarnings("deprecation") // getAwsRegion is deprecated in favor of getAwsRegionName
  public static @Nullable Region region(AwsOptions options) {
    Region region = options.getAwsRegion();
    String regionName = trimToNull(options.getAwsRegionName());
    if (regionName == null) {
      return region;
    }
    if (region != null && !region.id().equals(regionName)) {
      throw new IllegalArgumentException(
          String.format(
              "Conflicting AWS regions: --awsRegion=%s and --awsRegionName=%s. --awsRegion is "
                  + "deprecated, use only --awsRegionName.",
              region.id(), regionName));
    }
    return Region.of(regionName);
  }

  /**
   * Returns the effective endpoint: {@code --awsEndpointUrl} if set, otherwise the deprecated
   * {@code --endpoint}.
   *
   * @throws IllegalArgumentException if {@code --endpoint} and {@code --awsEndpointUrl} are set to
   *     different endpoints, or {@code --awsEndpointUrl} is not a valid URI.
   */
  @SuppressWarnings("deprecation") // getEndpoint is deprecated in favor of getAwsEndpointUrl
  public static @Nullable URI endpoint(AwsOptions options) {
    URI endpoint = options.getEndpoint();
    URI endpointUrl = parseEndpointUrl(options.getAwsEndpointUrl());
    if (endpointUrl == null) {
      return endpoint;
    }
    if (endpoint != null && !endpoint.equals(endpointUrl)) {
      throw new IllegalArgumentException(
          String.format(
              "Conflicting AWS endpoints: --endpoint=%s and --awsEndpointUrl=%s. --endpoint is "
                  + "deprecated, use only --awsEndpointUrl.",
              endpoint, endpointUrl));
    }
    return endpointUrl;
  }

  /**
   * Returns the effective credentials provider, i.e. {@link
   * AwsOptions#getAwsCredentialsProvider()}.
   *
   * <p>If not set explicitly, that option is derived from {@code --awsAccessKeyId}, {@code
   * --awsSecretAccessKey}, {@code --awsSessionToken} and {@code --awsCredentialsSecretManager}.
   *
   * @throws IllegalArgumentException if {@code --awsCredentialsProvider} is set explicitly together
   *     with {@code --awsAccessKeyId} and related options, or those options are incomplete.
   */
  public static @Nullable AwsCredentialsProvider credentialsProvider(AwsOptions options) {
    AwsCredentialsProvider provider = options.getAwsCredentialsProvider();
    AwsCredentialsProvider fromKeys = credentialsProviderFromKeys(options);
    // If the provider was derived from the key options (possibly before the options were
    // serialized), it is equivalent to fromKeys. Otherwise it was set explicitly.
    if (fromKeys != null && provider != null && !isEquivalent(fromKeys, provider)) {
      throw new IllegalArgumentException(
          "--awsCredentialsProvider cannot be combined with --awsAccessKeyId, "
              + "--awsSecretAccessKey, --awsSessionToken or --awsCredentialsSecretManager. "
              + "Use only one of them.");
    }
    return provider;
  }

  /**
   * Returns the credentials provider configured by {@code --awsAccessKeyId}, {@code
   * --awsSecretAccessKey}, {@code --awsSessionToken} and {@code --awsCredentialsSecretManager}, or
   * {@code null} if none of them is set.
   *
   * <p>With {@code --awsCredentialsSecretManager}, this is a {@link SecretCredentialsProvider}, so
   * the credentials are resolved lazily and only the secret specs are serialized. Otherwise, it is
   * a {@link StaticCredentialsProvider} with the raw credentials.
   *
   * @throws IllegalArgumentException if the options are incomplete.
   */
  static @Nullable AwsCredentialsProvider credentialsProviderFromKeys(AwsOptions options) {
    String accessKeyId = emptyToNull(options.getAwsAccessKeyId());
    String secretAccessKey = emptyToNull(options.getAwsSecretAccessKey());
    String sessionToken = emptyToNull(options.getAwsSessionToken());
    String secretManager = trimToNull(options.getAwsCredentialsSecretManager());

    if (accessKeyId == null && secretAccessKey == null) {
      if (sessionToken != null || secretManager != null) {
        throw new IllegalArgumentException(
            "--awsSessionToken and --awsCredentialsSecretManager require --awsAccessKeyId and "
                + "--awsSecretAccessKey.");
      }
      return null;
    }
    if (accessKeyId == null || secretAccessKey == null) {
      throw new IllegalArgumentException(
          "--awsAccessKeyId and --awsSecretAccessKey must be set together.");
    }
    if (secretManager != null) {
      return SecretCredentialsProvider.create(
          secretManager, accessKeyId, secretAccessKey, sessionToken);
    }
    AwsCredentials credentials =
        sessionToken == null
            ? AwsBasicCredentials.create(accessKeyId, secretAccessKey)
            : AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
    return StaticCredentialsProvider.create(credentials);
  }

  /** Parses {@code --awsEndpointUrl}, returning {@code null} if it is not set. */
  static @Nullable URI parseEndpointUrl(@Nullable String endpointUrl) {
    String url = trimToNull(endpointUrl);
    if (url == null) {
      return null;
    }
    try {
      return URI.create(url);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid --awsEndpointUrl: " + url, e);
    }
  }

  private static boolean isEquivalent(AwsCredentialsProvider a, AwsCredentialsProvider b) {
    if (a instanceof SecretCredentialsProvider || b instanceof SecretCredentialsProvider) {
      return a.equals(b);
    }
    if (a instanceof StaticCredentialsProvider && b instanceof StaticCredentialsProvider) {
      // Static credentials are resolved locally, without any remote call.
      AwsCredentials ca = a.resolveCredentials();
      AwsCredentials cb = b.resolveCredentials();
      return ca.accessKeyId().equals(cb.accessKeyId())
          && ca.secretAccessKey().equals(cb.secretAccessKey())
          && Objects.equals(sessionToken(ca), sessionToken(cb));
    }
    return false;
  }

  private static @Nullable String sessionToken(AwsCredentials credentials) {
    return credentials instanceof AwsSessionCredentials
        ? ((AwsSessionCredentials) credentials).sessionToken()
        : null;
  }

  private static @Nullable String emptyToNull(@Nullable String value) {
    return value == null || value.isEmpty() ? null : value;
  }

  private static @Nullable String trimToNull(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
