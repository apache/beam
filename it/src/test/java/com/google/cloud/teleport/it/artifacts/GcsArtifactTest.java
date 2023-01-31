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
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link GcsArtifact}. */
@RunWith(JUnit4.class)
public class GcsArtifactTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private Blob blob;
  private GcsArtifact artifact;

  @Before
  public void setUp() {
    artifact = new GcsArtifact(blob);
  }

  @Test
  public void testId() {
    String id = "test-id";
    when(blob.getGeneratedId()).thenReturn(id);
    assertThat(artifact.id()).isEqualTo(id);
  }

  @Test
  public void testName() {
    String name = "test-name";
    when(blob.getName()).thenReturn(name);
    assertThat(artifact.name()).isEqualTo(name);
  }

  @Test
  public void testContents() {
    byte[] contents = new byte[] {0, 1, 2};
    when(blob.getContent()).thenReturn(contents);
    assertThat(artifact.contents()).isEqualTo(contents);
  }
}
