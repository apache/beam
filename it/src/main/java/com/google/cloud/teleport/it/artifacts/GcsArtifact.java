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

import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;

/** Represents a single blob in GCS. */
public final class GcsArtifact implements Artifact {
  @VisibleForTesting final Blob blob;

  GcsArtifact(Blob blob) {
    this.blob = blob;
  }

  @Override
  public String id() {
    return blob.getGeneratedId();
  }

  @Override
  public String name() {
    return blob.getName();
  }

  @Override
  public byte[] contents() {
    return blob.getContent();
  }
}
