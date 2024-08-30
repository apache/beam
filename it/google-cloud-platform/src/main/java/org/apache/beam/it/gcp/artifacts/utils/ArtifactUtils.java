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
package org.apache.beam.it.gcp.artifacts.utils;

import static java.util.Arrays.stream;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** Utilities for working with test artifacts. */
public final class ArtifactUtils {
  private ArtifactUtils() {}

  /** Creates a unique id for the run. */
  public static String createRunId() {
    return String.format(
        "%s-%s",
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC")).format(Instant.now()),
        // by default some templates replace "dd" in the output paths with a day of the month, since
        // this id is used as a part of output paths replace "dd" with an arbitrary number to avoid
        // confusion and potential flaky tests
        UUID.randomUUID().toString().replace("dd", "99"));
  }

  /**
   * Returns the full GCS path given a list of path parts.
   *
   * <p>"path parts" refers to the bucket, directories, and file. Only the bucket is mandatory and
   * must be the first value provided.
   *
   * @param pathParts everything that makes up the path, minus the separators. There must be at
   *     least one value, and none of them can be empty
   * @return the full path, such as 'gs://bucket/dir1/dir2/file'
   */
  public static String getFullGcsPath(String... pathParts) {
    checkArgument(pathParts.length != 0, "Must provide at least one path part");
    checkArgument(
        stream(pathParts).noneMatch(Strings::isNullOrEmpty), "No path part can be null or empty");

    return String.format("gs://%s", String.join("/", pathParts));
  }

  /**
   * Creates a client for GCS with the given credentials.
   *
   * @param credentials credentials to use for connecting. If not chosen, then this will use the
   *     system credentials. Using system credentials is intended only for local testing. Otherwise,
   *     it is best to pass in a short-lived access token.
   * @return a {@link Storage} client for running GCS operations
   */
  public static Storage createStorageClient(Credentials credentials) {
    StorageOptions.Builder builder = StorageOptions.newBuilder();
    if (credentials != null) {
      builder.setCredentials(credentials);
    }
    return builder.build().getService();
  }
}
