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
package com.google.cloud.teleport.it.matchers;

import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Subject that has assertion operations for artifact lists (GCS files), usually coming from the
 * result of a template.
 */
public final class ArtifactsSubject extends Subject {

  @Nullable private final List<Artifact> actual;

  private ArtifactsSubject(FailureMetadata metadata, @Nullable List<Artifact> actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public static Factory<ArtifactsSubject, List<Artifact>> records() {
    return ArtifactsSubject::new;
  }

  /** Check if artifact list has files (is not empty). */
  public void hasFiles() {
    check("there are files").that(actual).isNotEmpty();
  }

  /**
   * Check if artifact list has a specific number of files.
   *
   * @param expectedFiles Expected Rows
   */
  public void hasFiles(int expectedFiles) {
    check("there are %d files", expectedFiles).that(actual.size()).isEqualTo(expectedFiles);
  }

  /**
   * Check if any of the artifacts has a specific content.
   *
   * @param content Content to search for
   */
  public void hasContent(String content) {
    if (!actual.stream().anyMatch(artifact -> new String(artifact.contents()).contains(content))) {
      failWithActual("expected to contain", content);
    }
  }
}
