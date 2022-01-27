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
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;

/** Interface for working with test artifacts. */
public interface ArtifactClient {
  /**
   * Uploads a local file to GCS.
   *
   * @param bucket the GCS bucket to upload to
   * @param gcsPath the path from the bucket root to upload to
   * @param localPath the path to the local file
   * @return the {@link Blob} that was created
   * @throws IOException if the local file cannot be read
   */
  Blob uploadArtifact(String bucket, String gcsPath, String localPath) throws IOException;

  /**
   * Lists all artifacts in the given directory that match a given regex.
   *
   * @param bucket the bucket the artifacts are in
   * @param testDirPath the directory in the bucket that the artifacts are in
   * @param regex the regex to use for matching artifacts
   * @return all the {@link Blob}s that match the regex
   */
  List<Blob> listArtifacts(String bucket, String testDirPath, Pattern regex);

  /**
   * Removes the directory from the bucket.
   *
   * @param bucket the bucket with the directory to remove
   * @param testDirPath the directory to remove
   */
  void deleteTestDir(String bucket, String testDirPath);
}
