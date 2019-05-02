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
package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

/** Naming policy for SMB files, on a per-bucket per-shard basis. */
public final class SMBFilenamePolicy implements Serializable {
  private static final String TEMPDIR_TIMESTAMP = "yyyy-MM-dd_HH-mm-ss";
  private static final String BEAM_TEMPDIR_PATTERN = ".temp-beam-%s";

  private final ResourceId filenamePrefix;
  private final String fileNameSuffix;

  public SMBFilenamePolicy(ResourceId destinationPrefix, String fileNameSuffix) {
    this.filenamePrefix = destinationPrefix;
    this.fileNameSuffix = fileNameSuffix;
  }

  public FileAssignment forDestination() {
    return new FileAssignment(filenamePrefix, fileNameSuffix);
  }

  public FileAssignment forTempFiles(ResourceId tempDirectory) {
    return new FileAssignment(
        tempDirectory.resolve(
            String.format(
                BEAM_TEMPDIR_PATTERN,
                Instant.now().toString(DateTimeFormat.forPattern(TEMPDIR_TIMESTAMP))),
            StandardResolveOptions.RESOLVE_DIRECTORY),
        fileNameSuffix,
        true);
  }

  /** A file name assigner based on a specific output directory and file suffix. */
  public static class FileAssignment implements Serializable {
    private static final String BUCKET_TEMPLATE = "bucket-%05d-of-%05d%s";
    private static final String METADATA_FILENAME = "metadata.json";
    private static final String TIMESTAMP_TEMPLATE = "yyyy-MM-dd_HH-mm-ss-";

    private final ResourceId filenamePrefix;
    private final String fileNameSuffix;
    private final boolean doTimestampFiles;

    FileAssignment(ResourceId filenamePrefix, String fileNameSuffix, boolean doTimestampFiles) {
      this.filenamePrefix = filenamePrefix;
      this.fileNameSuffix = fileNameSuffix;
      this.doTimestampFiles = doTimestampFiles;
    }

    FileAssignment(ResourceId filenamePrefix, String fileNameSuffix) {
      this(filenamePrefix, fileNameSuffix, false);
    }

    public ResourceId forBucket(int bucketNumber, int numBuckets) {
      String prefix = "";
      if (doTimestampFiles) {
        prefix += Instant.now().toString(DateTimeFormat.forPattern(TIMESTAMP_TEMPLATE));
      }

      return filenamePrefix.resolve(
          prefix + String.format(BUCKET_TEMPLATE, bucketNumber, numBuckets, fileNameSuffix),
          StandardResolveOptions.RESOLVE_FILE);
    }

    public ResourceId forMetadata() {
      return filenamePrefix.resolve(METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FileAssignment that = (FileAssignment) o;
      return doTimestampFiles == that.doTimestampFiles
          && Objects.equals(filenamePrefix, that.filenamePrefix)
          && Objects.equals(fileNameSuffix, that.fileNameSuffix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(filenamePrefix, fileNameSuffix, doTimestampFiles);
    }
  }
}
