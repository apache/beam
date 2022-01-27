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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TestProperties}. */
@RunWith(JUnit4.class)
public final class TestPropertiesTest {
  private static final String ACCESS_TOKEN = "some-token";
  private static final String ARTIFACT_BUCKET = "test-bucket";
  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String SPEC_PATH = "gs://test-bucket/some/spec/path";

  private final TestProperties properties = new TestProperties();

  @After
  public void tearDown() {
    System.clearProperty(TestProperties.ACCESS_TOKEN_KEY);
    System.clearProperty(TestProperties.ARTIFACT_BUCKET_KEY);
    System.clearProperty(TestProperties.PROJECT_KEY);
    System.clearProperty(TestProperties.REGION_KEY);
    System.clearProperty(TestProperties.SPEC_PATH_KEY);
  }

  @Test
  public void testAllPropertiesSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(properties.accessToken()).isEqualTo(ACCESS_TOKEN);
    assertThat(properties.artifactBucket()).isEqualTo(ARTIFACT_BUCKET);
    assertThat(properties.project()).isEqualTo(PROJECT);
    assertThat(properties.region()).isEqualTo(REGION);
    assertThat(properties.specPath()).isEqualTo(SPEC_PATH);
  }

  @Test
  public void testAccessTokenNotSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThrows(IllegalStateException.class, properties::accessToken);
  }

  @Test
  public void testArtifactBucketNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThrows(IllegalStateException.class, properties::artifactBucket);
  }

  @Test
  public void testProjectNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThrows(IllegalStateException.class, properties::project);
  }

  @Test
  public void testRegionNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(properties.region()).isEqualTo(TestProperties.DEFAULT_REGION);
  }

  @Test
  public void testSpecPathNotSet() {
    System.setProperty(TestProperties.ACCESS_TOKEN_KEY, ACCESS_TOKEN);
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);

    assertThrows(IllegalStateException.class, properties::specPath);
  }
}
