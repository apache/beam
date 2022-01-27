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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.truth.Truth;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Artifacts for {@link ArtifactUtils}. */
@RunWith(JUnit4.class)
public final class ArtifactUtilsTest {

  // Not matching exact date, since it may fail if the test runs close enough to the change of
  // date.
  private static final String TEST_DIR_REGEX =
      "\\d{8}-[a-fA-F0-9]{8}-([a-fA-F0-9]{4}-){3}[a-fA-F0-9]{12}";

  @Test
  public void testCreateTestDirName() {
    Truth.assertThat(ArtifactUtils.createTestDirName()).matches(TEST_DIR_REGEX);
  }

  @Test
  public void testCreateTestSuiteDirPath() {
    String suiteName = "some-test-class";
    String path = ArtifactUtils.createTestSuiteDirPath(suiteName);
    assertThat(path).matches(String.format("%s/%s", suiteName, TEST_DIR_REGEX));
  }

  @Test
  public void testCreateTestPath() {
    String suiteDirPath = "some/test/suite/dir";
    String testName = "some-test";

    String path = ArtifactUtils.createTestPath(suiteDirPath, testName);

    assertThat(path).matches(String.format("%s/%s", suiteDirPath, testName));
  }

  @Test
  public void testCreateClientWithNullCredentials() {
    ArtifactUtils.createGcsClient(null);
    // Just making sure that no exceptions are thrown
  }
}
