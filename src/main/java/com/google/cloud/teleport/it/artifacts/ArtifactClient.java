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

import com.google.re2j.Pattern;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Interface for working with test artifacts.
 *
 * <p>It is the responsibility of implementations to make sure that artifacts are kept separate from
 * each other. Using a GCS path, this isolation would create a path like the following: {@code
 * gs://test-class-name/run-id/test-method-name}. Each directory means:
 *
 * <ul>
 *   <li>test-class-name: A name given to the directory for a test class. This does not need to be
 *       identical to the class name, but it should clearly identify the class from other test
 *       classes. This is intended for long-lived artifacts that have value beyond a specific run of
 *       a test, such as a result file.
 *   <li>run-id: An id assigned to a run of that test class. This will be handled by implementations
 *       of this client. It is intended for artifacts that may be referenced by multiple methods in
 *       a test class.
 *   <li>test-method-name: A name given to the directory for a method within the test class. This
 *       does not need to be identical to the method name, but it should clearly identify the method
 *       from other test methods within the same test class. This is intended for input and output
 *       artifacts specific to the test method.
 * </ul>
 *
 * <p>Separate input/output directories are optional and the responsibility of the test writer to
 * maintain.
 */
public interface ArtifactClient {

  /** Returns the id associated with the particular run of the test class. */
  String runId();

  /**
   * Creates a new artifact in whatever service is being used to store them.
   *
   * @param artifactName the name of the artifact. If this is supposed to go under an input/output
   *     directory, then it should include that (example: input/artifact.txt)
   * @param contents the contents of the artifact
   * @return a representation of the created artifact
   */
  Artifact createArtifact(String artifactName, byte[] contents);

  /**
   * Uploads a local file to the service being used for storing artifacts.
   *
   * @param artifactName the name of the artifact. If this is supposed to go under an input/output
   *     directory, then it should include that (example: input/artifact.txt)
   * @param localPath the absolute local path to the file to upload
   * @return a representation of the uploaded artifact
   * @throws IOException if there is an issue reading the local file
   */
  Artifact uploadArtifact(String artifactName, String localPath) throws IOException;

  /**
   * Uploads a local file to the service being used for storing artifacts.
   *
   * @param artifactName the name of the artifact. If this is supposed to go under an input/output
   *     directory, then it should include that (example: input/artifact.txt)
   * @param localPath the local path to the file to upload
   * @return a representation of the uploaded artifact
   * @throws IOException if there is an issue reading the local file
   */
  Artifact uploadArtifact(String artifactName, Path localPath) throws IOException;

  // TODO(zhoufek): Add equivalents for the above for uploading artifacts of a test method

  /**
   * Lists all artifacts under test-class-name/run-id/{@code prefix}.
   *
   * @param prefix the prefix to use along with the fixed values method above. This must include the
   *     test-method-name value, but it can include other directories or files under it.
   * @param regex a regex to use for filtering out unwanted artifacts
   * @return all the artifacts whose name matches regex
   */
  List<Artifact> listArtifacts(String prefix, Pattern regex);

  /** Deletes all the files located under test-class-name/run-id. */
  void cleanupRun();
}
