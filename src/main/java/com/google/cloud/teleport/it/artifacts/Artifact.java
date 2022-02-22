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

/**
 * Represents a single artifact.
 *
 * <p>An "artifact" is an entity in object storage, file storage, or block storage. Artifacts should
 * be able to be stored in-memory as a single byte array. Implementations with an underlying type
 * that only supports streaming should stream in the full contents and make the full contents
 * available.
 *
 * <p>Implementations should remain read-only. Writing artifacts should be left to the
 * responsibility of a {@link ArtifactClient} implementation. If an object of the artifact type
 * returned allows writing of any type, then it should not be made available.
 */
public interface Artifact {
  /** Returns the id of the artifact. */
  String id();

  /** Returns the name/path of the artifact. */
  String name();

  /** Returns the raw byte array of the artifact's contents. */
  byte[] contents();
}
