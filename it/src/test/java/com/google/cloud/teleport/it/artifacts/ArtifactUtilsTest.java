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
import static org.junit.Assert.assertThrows;

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
    assertThat(ArtifactUtils.createRunId()).matches(TEST_DIR_REGEX);
  }

  @Test
  public void testGetFullGcsPath() {
    assertThat(ArtifactUtils.getFullGcsPath("bucket", "dir1", "dir2", "file"))
        .isEqualTo("gs://bucket/dir1/dir2/file");
  }

  @Test
  public void testGetFullGcsPathOnlyBucket() {
    assertThat(ArtifactUtils.getFullGcsPath("bucket")).isEqualTo("gs://bucket");
  }

  @Test
  public void testGetFullGcsPathEmpty() {
    assertThrows(IllegalArgumentException.class, ArtifactUtils::getFullGcsPath);
  }

  @Test
  public void testGetFullGcsPathOneNullValue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArtifactUtils.getFullGcsPath("bucket", null, "dir2", "file"));
  }

  @Test
  public void testGetFullGcsPathOneEmptyValue() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ArtifactUtils.getFullGcsPath("bucket", "", "dir2", "file"));
  }
}
