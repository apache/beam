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
package org.apache.beam.sdk.extensions.opentelemetry.gcp.auth;

import static java.util.Locale.ROOT;

import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigurationException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * An enum representing configurable options for a GCP Authentication Extension. Each option has a
 * user-readable name and can be configured using environment variables or system properties.
 *
 * <p>Copied from
 * https://github.com/open-telemetry/opentelemetry-java-contrib/blob/main/gcp-auth-extension/src/main/java/io/opentelemetry/contrib/gcp/auth/ConfigurableOption.java
 */
enum ConfigurableOption {
  /**
   * Represents the Google Cloud Project ID option. Can be configured using the environment variable
   * `GOOGLE_CLOUD_PROJECT` or the system property `google.cloud.project`.
   */
  GOOGLE_CLOUD_PROJECT("Google Cloud Project ID"),

  /**
   * Represents the Google Cloud Quota Project ID option. Can be configured using the environment
   * variable `GOOGLE_CLOUD_QUOTA_PROJECT` or the system property `google.cloud.quota.project`. The
   * quota project is the project that is used for quota management and billing for the API usage.
   *
   * <p>The environment variable name is selected to be consistent with the <a
   * href="https://cloud.google.com/docs/quotas/set-quota-project">official GCP client
   * libraries</a>.
   */
  GOOGLE_CLOUD_QUOTA_PROJECT("Google Cloud Quota Project ID"),

  /**
   * Specifies a comma-separated list of OpenTelemetry signals for which this authentication
   * extension should be active. The authentication mechanisms provided by this extension will only
   * be applied to the listed signals. If not set, {@code all} is assumed to be set which means
   * authentication is enabled for all supported signals.
   *
   * <p>Valid signal values are:
   *
   * <ul>
   *   <li>{@code metrics} - Enables authentication for metric exports.
   *   <li>{@code traces} - Enables authentication for trace exports.
   *   <li>{@code all} - Enables authentication for all exports.
   * </ul>
   *
   * <p>The values are case-sensitive. Whitespace around commas and values is ignored. Can be
   * configured using the environment variable `GOOGLE_OTEL_AUTH_TARGET_SIGNALS` or the system
   * property `google.otel.auth.target.signals`.
   */
  GOOGLE_OTEL_AUTH_TARGET_SIGNALS("Target Signals for Google Authentication Extension");

  private final String userReadableName;
  private final String environmentVariableName;
  private final String systemPropertyName;

  ConfigurableOption(String userReadableName) {
    this.userReadableName = userReadableName;
    this.environmentVariableName = this.name();
    this.systemPropertyName = this.environmentVariableName.toLowerCase(ROOT).replace('_', '.');
  }

  /**
   * Returns the environment variable name associated with this option.
   *
   * @return the environment variable name (e.g., GOOGLE_CLOUD_PROJECT)
   */
  String getEnvironmentVariable() {
    return this.environmentVariableName;
  }

  /**
   * Returns the system property name associated with this option.
   *
   * @return the system property name (e.g., google.cloud.project)
   */
  String getSystemProperty() {
    return this.systemPropertyName;
  }

  /**
   * Returns the user readable name associated with this option.
   *
   * @return the user readable name (e.g., "Google Cloud Quota Project ID")
   */
  String getUserReadableName() {
    return this.userReadableName;
  }

  /**
   * Retrieves the configured value for this option. This method checks the environment variable
   * first and then the system property.
   *
   * @return The configured value as a string, or throws an exception if not configured.
   * @throws ConfigurationException if neither the environment variable nor the system property is
   *     set.
   */
  String getConfiguredValue(ConfigProperties configProperties) {
    String configuredValue = configProperties.getString(this.getSystemProperty());
    if (configuredValue != null && !configuredValue.isEmpty()) {
      return configuredValue;
    } else {
      throw new ConfigurationException(
          String.format(
              "GCP Authentication Extension not configured properly: %s not configured. Configure it by exporting environment variable %s or system property %s",
              this.userReadableName, this.getEnvironmentVariable(), this.getSystemProperty()));
    }
  }

  /**
   * Retrieves the value for this option, prioritizing environment variables and system properties.
   * If neither an environment variable nor a system property is set for this option, the provided
   * fallback function is used to determine the value.
   *
   * @param fallback A {@link Supplier} that provides the default value for the option when it is
   *     not explicitly configured via an environment variable or system property.
   * @return The configured value for the option, obtained from the environment variable, system
   *     property, or the fallback function, in that order of precedence.
   */
  String getConfiguredValueWithFallback(
      ConfigProperties configProperties, Supplier<String> fallback) {
    try {
      return this.getConfiguredValue(configProperties);
    } catch (ConfigurationException e) {
      return fallback.get();
    }
  }

  /**
   * Retrieves the value for this option, prioritizing environment variables before system
   * properties. If neither an environment variable nor a system property is set for this option,
   * then an empty {@link Optional} is returned.
   *
   * @return The configured value for the option, if set, obtained from the environment variable,
   *     system property, or empty {@link Optional}, in that order of precedence.
   */
  Optional<String> getConfiguredValueAsOptional(ConfigProperties configProperties) {
    try {
      return Optional.of(this.getConfiguredValue(configProperties));
    } catch (ConfigurationException e) {
      return Optional.empty();
    }
  }
}
