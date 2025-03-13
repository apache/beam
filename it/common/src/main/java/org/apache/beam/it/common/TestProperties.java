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
package org.apache.beam.it.common;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for accessing system properties set for the test.
 *
 * <p>There are two types of properties: those set on the command lines and those set as environment
 * variables. Those set on the command line always follow a camelCase naming convention, and those
 * set as environment variable always follow a CAPITALIZED_SNAKE_CASE naming convention.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
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

  private static final Logger LOG = LoggerFactory.getLogger(TestProperties.class);

  public static boolean hasAccessToken() {
    return getProperty(ACCESS_TOKEN_KEY, null, Type.ENVIRONMENT_VARIABLE) != null;
  }

  public static String accessToken() {
    return getProperty(ACCESS_TOKEN_KEY, Type.ENVIRONMENT_VARIABLE, true);
  }

  /**
   * Create and return credentials based on whether access token was provided or not.
   *
   * <p>If access token was provided, use the token for Bearer authentication.
   *
   * <p>If not, use Application Default Credentials. Check
   * https://cloud.google.com/docs/authentication/application-default-credentials for more
   * information.
   *
   * @return Credentials.
   */
  public static Credentials credentials() {
    if (hasAccessToken()) {
      return googleCredentials();
    } else {
      return buildCredentialsFromEnv();
    }
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
    return bucketNameOnly(getProperty(ARTIFACT_BUCKET_KEY, Type.PROPERTY, true));
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
    return bucketNameOnly(getProperty(STAGE_BUCKET, Type.PROPERTY, false));
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

  /**
   * Infers the {@link Credentials} to use with Google services from the current environment
   * settings.
   *
   * <p>First, checks if {@link ServiceAccountCredentials#getApplicationDefault()} returns Compute
   * Engine credentials, which means that it is running from a GCE instance and can use the Service
   * Account configured for that VM. Will use that
   *
   * <p>Secondly, it will try to get the environment variable
   * <strong>GOOGLE_APPLICATION_CREDENTIALS</strong>, and use that Service Account if configured to
   * doing so. The method {@link #getCredentialsStream()} will make sure to search for the specific
   * file using both the file system and classpath.
   *
   * <p>If <strong>GOOGLE_APPLICATION_CREDENTIALS</strong> is not configured, it will return the
   * application default, which is often setup through <strong>gcloud auth application-default
   * login</strong>.
   */
  public static Credentials buildCredentialsFromEnv() {
    try {

      // if on Compute Engine, return default credentials.
      GoogleCredentials applicationDefault = ServiceAccountCredentials.getApplicationDefault();
      if (applicationDefault instanceof ComputeEngineCredentials) {
        return applicationDefault;
      }

      InputStream credentialsStream = getCredentialsStream();
      if (credentialsStream == null) {
        return applicationDefault;
      }
      return ServiceAccountCredentials.fromStream(credentialsStream);
    } catch (IOException e) {
      throw new RuntimeException(
          "Unable to get application default credentials! \n"
              + "Check https://cloud.google.com/docs/authentication/application-default-credentials for more information.");
    }
  }

  private static InputStream getCredentialsStream() throws FileNotFoundException {
    String credentialFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

    if (credentialFile == null || credentialFile.isEmpty()) {
      LOG.warn(
          "Not found Google Cloud credentials: GOOGLE_APPLICATION_CREDENTIALS, assuming application"
              + " default");
      return null;
    }

    InputStream is = null;

    File credentialFileRead = new File(credentialFile);
    if (credentialFileRead.exists()) {
      is = new FileInputStream(credentialFile);
    }

    if (is == null) {
      is = TestProperties.class.getResourceAsStream(credentialFile);
    }

    if (is == null) {
      is = TestProperties.class.getResourceAsStream("/" + credentialFile);
    }

    if (is == null) {
      LOG.warn("Not found credentials with file name {}", credentialFile);
      return null;
    }
    return is;
  }

  /**
   * There are cases in which users will pass a gs://{bucketName} or a gs://{bucketName}/path
   * wrongly to a bucket name property. This will ensure that execution will run as expected
   * considering some input variations.
   *
   * @param bucketName User input with the bucket name.
   * @return Bucket name if parseable, or throw exception otherwise.
   * @throws IllegalArgumentException If bucket name can not be handled as such.
   */
  public static String bucketNameOnly(String bucketName) {

    String changedName = bucketName;
    // replace leading gs://
    if (changedName.startsWith("gs://")) {
      changedName = changedName.replaceFirst("gs://", "");
    }
    // replace trailing slash
    if (changedName.endsWith("/")) {
      changedName = changedName.replaceAll("/$", "");
    }

    if (changedName.contains("/") || changedName.contains(":")) {
      throw new IllegalArgumentException(
          "Bucket name "
              + bucketName
              + " is invalid. It should only contain the name of the bucket (not a path or URL).");
    }

    return changedName;
  }
}
