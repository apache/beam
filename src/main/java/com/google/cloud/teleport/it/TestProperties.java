/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it;

import static com.google.common.base.Preconditions.checkState;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import javax.annotation.Nullable;

/**
 * Utility for accessing system properties set for the test.
 *
 * <p>There are two types of properties: those set on the command lines and those set as environment
 * variables. Those set on the command line always follow a camelCase naming convention, and those
 * set as environment variable always follow a CAPITALIZED_SNAKE_CASE naming convention.
 */
public final class TestProperties {
  private TestProperties() {}

  // For testability, it is normally best to expect each property from the command line. We should
  // only expect an environment variable if we're trying to avoid an accidental log of the
  // value.

  // From command line
  public static final String ARTIFACT_BUCKET_KEY = "artifactBucket";
  public static final String PROJECT_KEY = "project";
  public static final String REGION_KEY = "region";
  public static final String SPEC_PATH_KEY = "specPath";

  // From environment variables
  public static final String ACCESS_TOKEN_KEY = "DT_IT_ACCESS_TOKEN";

  // Default values for optional properties
  public static final String DEFAULT_REGION = "us-central1";

  // Error messages
  private static final String CLI_ERR_MSG = "-D%s is required on the command line";
  private static final String ENV_VAR_MSG = "%s is required as an environment variable";

  public static String accessToken() {
    return getProperty(ACCESS_TOKEN_KEY, Type.ENVIRONMENT_VARIABLE);
  }

  public static Credentials googleCredentials() {
    return new GoogleCredentials(new AccessToken(accessToken(), /* expirationTime= */ null));
  }

  public static String artifactBucket() {
    return getProperty(ARTIFACT_BUCKET_KEY, Type.PROPERTY);
  }

  public static String project() {
    return getProperty(PROJECT_KEY, Type.PROPERTY);
  }

  public static String region() {
    return getProperty(REGION_KEY, DEFAULT_REGION, Type.PROPERTY);
  }

  public static String specPath() {
    return getProperty(SPEC_PATH_KEY, Type.PROPERTY);
  }

  /** Gets a property or throws an exception if it is not found. */
  private static String getProperty(String name, Type type) {
    String value = getProperty(name, null, type);
    String errMsg =
        type == Type.PROPERTY ? String.format(CLI_ERR_MSG, name) : String.format(ENV_VAR_MSG, name);

    checkState(value != null, errMsg);
    return value;
  }

  /** Gets a property or returns {@code defaultValue} if it is not found. */
  private static String getProperty(String name, @Nullable String defaultValue, Type type) {
    String value = type == Type.PROPERTY ? System.getProperty(name) : System.getenv(name);
    return value != null ? value : defaultValue;
  }

  /** Defines the types of properties there may be. */
  private enum Type {
    PROPERTY,
    ENVIRONMENT_VARIABLE
  }
}
