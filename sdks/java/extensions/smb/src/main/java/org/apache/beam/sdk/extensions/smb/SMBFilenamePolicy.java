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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Naming policy for SMB files, on a per-bucket basis. */
public final class SMBFilenamePolicy implements Serializable {

  private static final String TEMP_DIRECTORY_PREFIX = ".temp-beam";
  private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
  private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
      DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss");

  private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
  private final Long tempId = TEMP_COUNT.getAndIncrement();

  private final ResourceId filenamePrefix;
  private final String filenameSuffix;

  public SMBFilenamePolicy(ResourceId filenamePrefix, String filenameSuffix) {
    Preconditions.checkArgument(
        filenamePrefix.isDirectory(), "The ResourceId for filenamePrefix must be a directory");
    this.filenamePrefix = filenamePrefix;
    this.filenameSuffix = filenameSuffix;
  }

  FileAssignment forDestination() {
    return new FileAssignment(filenamePrefix, filenameSuffix);
  }

  FileAssignment forTempFiles(ResourceId tempDirectory) {
    final String tempDirName =
        String.format(TEMP_DIRECTORY_PREFIX + "-%s-%s", timestamp, getTempId());
    return new FileAssignment(
        tempDirectory
            .getCurrentDirectory()
            .resolve(tempDirName, StandardResolveOptions.RESOLVE_DIRECTORY),
        filenameSuffix,
        true);
  }

  @VisibleForTesting
  Long getTempId() {
    return tempId;
  }

  /** A file name assigner based on a specific output directory and file suffix. */
  public static class FileAssignment implements Serializable {

    private static final String NULL_KEYS_BUCKET_TEMPLATE = "null-keys";
    private static final String NUMERIC_BUCKET_TEMPLATE = "%05d-of-%05d";
    private static final String BUCKET_SHARD_TEMPLATE = "bucket-%s-shard-%05d-of-%05d%s";
    private static final String METADATA_FILENAME = "metadata.json";
    private static final DateTimeFormatter TEMPFILE_TIMESTAMP =
        DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss-");

    private final ResourceId filenamePrefix;
    private final String filenameSuffix;
    private final boolean doTimestampFiles;

    FileAssignment(ResourceId filenamePrefix, String filenameSuffix, boolean doTimestampFiles) {
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
      this.doTimestampFiles = doTimestampFiles;
    }

    FileAssignment(ResourceId filenamePrefix, String filenameSuffix) {
      this(filenamePrefix, filenameSuffix, false);
    }

    ResourceId forBucket(BucketShardId id, BucketMetadata<?, ?> metadata) {
      Preconditions.checkArgument(
          id.getBucketId() < metadata.getNumBuckets(),
          "Can't assign a filename for bucketShardId %s: max number of buckets is %s",
          id,
          metadata.getNumBuckets());

      Preconditions.checkArgument(
          id.getShardId() < metadata.getNumShards(),
          "Can't assign a filename for bucketShardId %s: max number of shards is %s",
          id,
          metadata.getNumBuckets());

      final String bucketName =
          id.isNullKeyBucket()
              ? NULL_KEYS_BUCKET_TEMPLATE
              : String.format(NUMERIC_BUCKET_TEMPLATE, id.getBucketId(), metadata.getNumBuckets());

      final String timestamp = doTimestampFiles ? Instant.now().toString(TEMPFILE_TIMESTAMP) : "";
      String filename =
          String.format(
              BUCKET_SHARD_TEMPLATE,
              bucketName,
              id.getShardId(),
              metadata.getNumShards(),
              filenameSuffix);
      return filenamePrefix.resolve(timestamp + filename, StandardResolveOptions.RESOLVE_FILE);
    }

    ResourceId forMetadata() {
      String timestamp = doTimestampFiles ? Instant.now().toString(TEMPFILE_TIMESTAMP) : "";
      return filenamePrefix.resolve(
          timestamp + METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
    }
  }
}
