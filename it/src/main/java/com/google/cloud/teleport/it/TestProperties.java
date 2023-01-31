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
import com.google.cloud.teleport.metadata.util.MetadataUtils;
import java.io.IOException;
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
  public static final String STAGE_BUCKET = "stageBucket";
  public static final String EXPORT_DATASET_KEY = "exportDataset";
  public static final String EXPORT_PROJECT = "exportProject";
  public static final String EXPORT_TABLE_KEY = "exportTable";
  public static final String SPEC_PATH_KEY = "specPath";
  public static final String HOST_IP = "hostIp";

  // From environment variables
  public static final String ACCESS_TOKEN_KEY = "DT_IT_ACCESS_TOKEN";

  // Default values for optional properties
  public static final String DEFAULT_REGION = "us-central1";

  // Error messages
  private static final String CLI_ERR_MSG = "-D%s is required on the command line";
  private static final String ENV_VAR_MSG = "%s is required as an environment variable";

  public static boolean hasAccessToken() {
    return getProperty(ACCESS_TOKEN_KEY, null, Type.ENVIRONMENT_VARIABLE) != null;
  }

  public static String accessToken() {
    return getProperty(ACCESS_TOKEN_KEY, Type.ENVIRONMENT_VARIABLE, true);
  }

  public static Credentials googleCredentials() {
    Credentials credentials;
    try {
      if (hasAccessToken()) {
        credentials =
            new GoogleCredentials(new AccessToken(accessToken(), /* expirationTime= */ null));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to get credentials! \n"
              + "Please run the following command to set 60 minute access token, \n"
              + "\t export DT_IT_ACCESS_TOKEN=$(gcloud auth application-default print-access-token) \n"
              + "Please run the following command to set credentials using the gcloud command, "
              + "\t gcloud auth application-default login");
    }
    return credentials;
  }

  public static boolean hasArtifactBucket() {
    return getProperty(ARTIFACT_BUCKET_KEY, null, Type.PROPERTY) != null;
  }

  public static String artifactBucket() {
    return MetadataUtils.bucketNameOnly(getProperty(ARTIFACT_BUCKET_KEY, Type.PROPERTY, true));
  }

  public static String exportDataset() {
    return getProperty(EXPORT_DATASET_KEY, Type.PROPERTY, false);
  }

  public static String exportProject() {
    return getProperty(EXPORT_PROJECT, Type.PROPERTY, false);
  }

  public static String exportTable() {
    return getProperty(EXPORT_TABLE_KEY, Type.PROPERTY, false);
  }

  public static String project() {
    return getProperty(PROJECT_KEY, Type.PROPERTY, true);
  }

  public static String region() {
    return getProperty(REGION_KEY, DEFAULT_REGION, Type.PROPERTY);
  }

  public static String specPath() {
    return getProperty(SPEC_PATH_KEY, Type.PROPERTY, false);
  }

  public static boolean hasStageBucket() {
    return getProperty(STAGE_BUCKET, null, Type.PROPERTY) != null;
  }

  public static String stageBucket() {
    return MetadataUtils.bucketNameOnly(getProperty(STAGE_BUCKET, Type.PROPERTY, false));
  }

  public static String hostIp() {
    return getProperty(HOST_IP, "localhost", Type.PROPERTY);
  }

  /** Gets a property or throws an exception if it is not found. */
  private static String getProperty(String name, Type type, boolean required) {
    String value = getProperty(name, null, type);

    if (required) {
      String errMsg =
          type == Type.PROPERTY
              ? String.format(CLI_ERR_MSG, name)
              : String.format(ENV_VAR_MSG, name);
      checkState(value != null, errMsg);
    }

    return value;
  }

  /** Gets a property or returns {@code defaultValue} if it is not found. */
  public static String getProperty(String name, @Nullable String defaultValue, Type type) {
    String value = type == Type.PROPERTY ? System.getProperty(name) : System.getenv(name);
    return value != null ? value : defaultValue;
  }

  /** Defines the types of properties there may be. */
  public enum Type {
    PROPERTY,
    ENVIRONMENT_VARIABLE
  }
}
