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
package org.apache.beam.sdk.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Secret} manager implementation that retrieves secrets from Google Cloud Secret Manager.
 */
public class GcpSecret extends Secret {
  private static final Logger LOG = LoggerFactory.getLogger(GcpSecret.class);
  private final String versionName;

  /**
   * Initializes a {@link GcpSecret} object.
   *
   * @param versionName The full version name of the secret in Google Cloud Secret Manager. For
   *     example: projects/<id>/secrets/<secret_name>/versions/1. For more info, see
   *     https://cloud.google.com/python/docs/reference/secretmanager/latest/google.cloud.secretmanager_v1beta1.services.secret_manager_service.SecretManagerServiceClient#google_cloud_secretmanager_v1beta1_services_secret_manager_service_SecretManagerServiceClient_access_secret_version
   */
  public GcpSecret(String versionName) {
    this.versionName = versionName;
  }

  /** Initialize GcpSecret from a map specification. */
  static GcpSecret fromMap(Map<String, String> specMap) {
    Set<String> allowedKeys =
        new HashSet<>(Arrays.asList("version_name", "name", "project", "version"));
    Set<String> invalidKeys = new HashSet<>(specMap.keySet());
    invalidKeys.removeAll(allowedKeys);
    if (!invalidKeys.isEmpty()) {
      List<String> sortedInvalid = new ArrayList<>(invalidKeys);
      Collections.sort(sortedInvalid);
      throw new IllegalArgumentException(
          "Invalid secret parameter " + String.join(", ", sortedInvalid));
    }
    String versionName = parseVersionName(specMap);
    return new GcpSecret(versionName);
  }

  /** Parses the version name from a specification dictionary. */
  private static String parseVersionName(Map<String, String> specMap) {
    String versionNameParam = specMap.get("version_name");
    if (!Strings.isNullOrEmpty(versionNameParam)) {
      return Preconditions.checkNotNull(
          versionNameParam, "version_name must contain a valid value for versionName parameter");
    }
    String secretId = specMap.get("name");
    if (Strings.isNullOrEmpty(secretId)) {
      throw new IllegalArgumentException("Secret name must be specified in secret spec.");
    }
    String projectId = resolveGcpProjectId(specMap.get("project"), "secret '" + secretId + "'");
    String versionId = specMap.getOrDefault("version", "latest");
    if (Strings.isNullOrEmpty(versionId)) {
      versionId = "latest";
    }
    return String.format("projects/%s/secrets/%s/versions/%s", projectId, secretId, versionId);
  }

  /**
   * Resolves the GCP project ID from the provided value, environment variables, or Application
   * Default Credentials.
   */
  static String resolveGcpProjectId(@Nullable String projectId, @Nullable String context) {
    if (!Strings.isNullOrEmpty(projectId)) {
      return Preconditions.checkNotNull(projectId);
    }
    String envProject = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (!Strings.isNullOrEmpty(envProject)) {
      return Preconditions.checkNotNull(envProject);
    }
    envProject = System.getenv("GCP_PROJECT");
    if (!Strings.isNullOrEmpty(envProject)) {
      return Preconditions.checkNotNull(envProject);
    }
    try {
      Class<?> clazz = Class.forName("com.google.cloud.ServiceOptions");
      java.lang.reflect.Method method = clazz.getMethod("getDefaultProjectId");
      @SuppressWarnings("nullness")
      Object result = method.invoke(null);
      if (result != null && !Strings.isNullOrEmpty(result.toString())) {
        return result.toString();
      }
    } catch (Throwable e) {
      LOG.debug("Could not resolve GCP project via ServiceOptions reflection", e);
    }
    throw new IllegalArgumentException(
        String.format(
            "Could not resolve GCP project ID%s. "
                + "Please specify 'project' in the secret spec, set GOOGLE_CLOUD_PROJECT environment variable, "
                + "or configure Application Default Credentials.",
            context != null ? " for " + context : ""));
  }

  /**
   * Returns the secret as a byte array. Assumes that the current active service account has
   * permissions to read the secret.
   *
   * @return The secret as a byte array.
   */
  @Override
  public byte[] getSecretBytes() {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      SecretVersionName secretVersionName = SecretVersionName.parse(versionName);
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to retrieve secret bytes", e);
    }
  }

  /**
   * Returns the version name of the secret.
   *
   * @return The version name as a String.
   */
  public String getVersionName() {
    return versionName;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GcpSecret)) {
      return false;
    }
    GcpSecret other = (GcpSecret) obj;
    return Objects.equals(this.versionName, other.versionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionName);
  }
}
