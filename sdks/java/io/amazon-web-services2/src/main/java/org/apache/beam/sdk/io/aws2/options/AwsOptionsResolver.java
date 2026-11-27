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
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.regions.Region;

/**
 * Resolves the effective AWS client settings from {@link AwsOptions}.
 *
 * <p>{@link AwsOptions} has deprecated options ({@code --awsRegion}, {@code --endpoint}) that
 * overlap with newer options ({@code --awsRegionName}, {@code --awsEndpointUrl}). The methods of
 * this class return the effective setting and fail if overlapping options are set to conflicting
 * values.
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

  private static @Nullable String trimToNull(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
