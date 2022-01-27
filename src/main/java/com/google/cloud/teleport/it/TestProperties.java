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
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility for accessing system properties set for the test.
 *
 * <p>The values should be passed to the test like `-Dkey=value`. For instance,
 * `-Dproject=my-project`.
 */
public final class TestProperties {
  public static final String ACCESS_TOKEN_KEY = "accessToken";
  public static final String ARTIFACT_BUCKET_KEY = "artifactBucket";
  public static final String PROJECT_KEY = "project";
  public static final String REGION_KEY = "region";
  public static final String SPEC_PATH_KEY = "specPath";

  public static final String DEFAULT_REGION = "us-central1";

  private static String accessToken;
  private static String artifactBucket;
  private static String project;
  private static String region;
  private static String specPath;

  private final Map<String, Boolean> initialized;

  public TestProperties() {
    initialized = new HashMap<>();
    initialized.put(ACCESS_TOKEN_KEY, false);
    initialized.put(ARTIFACT_BUCKET_KEY, false);
    initialized.put(PROJECT_KEY, false);
    initialized.put(REGION_KEY, false);
    initialized.put(SPEC_PATH_KEY, false);
  }

  public String accessToken() {
    if (!initialized.get(ACCESS_TOKEN_KEY)) {
      accessToken = System.getProperty(ACCESS_TOKEN_KEY, null);
      checkState(!Strings.isNullOrEmpty(accessToken), "%s is required", ACCESS_TOKEN_KEY);
      initialized.replace(ACCESS_TOKEN_KEY, true);
    }
    return accessToken;
  }

  public Credentials googleCredentials() {
    return new GoogleCredentials(new AccessToken(accessToken(), /* expirationTime= */ null));
  }

  public String artifactBucket() {
    if (!initialized.get(ARTIFACT_BUCKET_KEY)) {
      artifactBucket = System.getProperty(ARTIFACT_BUCKET_KEY, null);
      checkState(!Strings.isNullOrEmpty(artifactBucket), "%s is required", ARTIFACT_BUCKET_KEY);
      initialized.replace(ARTIFACT_BUCKET_KEY, true);
    }
    return artifactBucket;
  }

  public String project() {
    if (!initialized.get(PROJECT_KEY)) {
      project = System.getProperty(PROJECT_KEY, null);
      checkState(!Strings.isNullOrEmpty(project), "%s is required", PROJECT_KEY);
      initialized.replace(PROJECT_KEY, true);
    }
    return project;
  }

  public String region() {
    if (!initialized.get(REGION_KEY)) {
      region = System.getProperty(REGION_KEY, DEFAULT_REGION);
      initialized.replace(REGION_KEY, true);
    }
    return region;
  }

  public String specPath() {
    if (!initialized.get(SPEC_PATH_KEY)) {
      specPath = System.getProperty(SPEC_PATH_KEY, null);
      checkState(!Strings.isNullOrEmpty(specPath), "%s is required", SPEC_PATH_KEY);
      initialized.replace(SPEC_PATH_KEY, true);
    }
    return specPath;
  }
}
