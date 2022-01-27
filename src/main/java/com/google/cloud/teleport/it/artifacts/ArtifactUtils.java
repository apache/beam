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
package com.google.cloud.teleport.it.artifacts;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import javax.annotation.Nullable;

/** Utilities for working with test artifacts. */
public final class ArtifactUtils {
  private ArtifactUtils() {}

  /** Creates a unique name for the test directory. */
  public static String createTestDirName() {
    return String.format(
        "%s-%s",
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC")).format(Instant.now()),
        UUID.randomUUID());
  }

  /**
   * Creates a unique path for the test suite.
   *
   * @param suiteDir the name of the test suite. This is generally the class with all the tests in
   *     it.
   */
  public static String createTestSuiteDirPath(String suiteDir) {
    return String.format("%s/%s", suiteDir, createTestDirName());
  }

  /**
   * Creates a path for artifacts from an individual test to go into.
   *
   * @param suiteDirPath the name of the test suite. This is generally the class with all the tests
   *     in it.
   * @param testName the name of the test. It is the responsibility of the caller to make sure all
   *     their test names are unique.
   */
  public static String createTestPath(String suiteDirPath, String testName) {
    return String.format("%s/%s", suiteDirPath, testName);
  }

  /**
   * Creates a client for GCS with the given credentials.
   *
   * @param credentials credentials to use for connecting. If not chosen, then this will use the
   *     system credentials. Using system credentials is intended only for local testing. Otherwise,
   *     it is best to pass in a short-lived access token.
   * @return a {@link Storage} client for running GCS operations
   */
  public static Storage createGcsClient(@Nullable Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }
}
