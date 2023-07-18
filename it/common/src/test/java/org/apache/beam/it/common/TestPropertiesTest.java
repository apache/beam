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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TestProperties}. */
@RunWith(JUnit4.class)
public final class TestPropertiesTest {
  private static final String ARTIFACT_BUCKET = "test-bucket";
  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String SPEC_PATH = "gs://test-bucket/some/spec/path";

  @After
  public void tearDown() {
    System.clearProperty(TestProperties.ARTIFACT_BUCKET_KEY);
    System.clearProperty(TestProperties.PROJECT_KEY);
    System.clearProperty(TestProperties.REGION_KEY);
    System.clearProperty(TestProperties.SPEC_PATH_KEY);
  }

  @Test
  public void testAllPropertiesSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(TestProperties.artifactBucket()).isEqualTo(ARTIFACT_BUCKET);
    assertThat(TestProperties.project()).isEqualTo(PROJECT);
    assertThat(TestProperties.region()).isEqualTo(REGION);
    assertThat(TestProperties.specPath()).isEqualTo(SPEC_PATH);
  }

  @Test
  public void testArtifactBucketNotSet() {
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThrows(IllegalStateException.class, TestProperties::artifactBucket);
  }

  @Test
  public void testProjectNotSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.REGION_KEY, REGION);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThrows(IllegalStateException.class, TestProperties::project);
  }

  @Test
  public void testRegionNotSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.SPEC_PATH_KEY, SPEC_PATH);

    assertThat(TestProperties.region()).isEqualTo(TestProperties.DEFAULT_REGION);
  }

  @Test
  public void testSpecPathNotSet() {
    System.setProperty(TestProperties.ARTIFACT_BUCKET_KEY, ARTIFACT_BUCKET);
    System.setProperty(TestProperties.PROJECT_KEY, PROJECT);
    System.setProperty(TestProperties.REGION_KEY, REGION);

    assertThat(TestProperties.specPath()).isNull();
  }
}
