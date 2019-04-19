package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

// @todo.... need to think about this more, a lot of redundant info getting passed around
public final class SMBFilenamePolicy implements Serializable {
  private static final String TEMPDIR_TIMESTAMP = "yyyy-MM-dd_HH-mm-ss";
  private static final String BEAM_TEMPDIR_PATTERN = ".temp-beam-%s";

  private final ResourceId filenamePrefix;
  private final ResourceId tempDirectory;
  private final String fileNameSuffix;

  public SMBFilenamePolicy(
      ResourceId destinationPrefix,
      String fileNameSuffix,
      ResourceId tempDirectory
  ) {
    this.filenamePrefix = destinationPrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.tempDirectory = tempDirectory;
  }

  public FileAssignment forDestination() {
    return new FileAssignment(filenamePrefix, fileNameSuffix, false);
  }

  public FileAssignment forTempFiles() {
    return new FileAssignment(
        tempDirectory.resolve(
          String.format(
              BEAM_TEMPDIR_PATTERN,
              Instant.now().toString(DateTimeFormat.forPattern(TEMPDIR_TIMESTAMP))
          ), StandardResolveOptions.RESOLVE_DIRECTORY),
        fileNameSuffix,
        true);
  }

  static class FileAssignment implements Serializable {
    private static final String BUCKET_TEMPLATE = "bucket-%d-of-%d-shard-%d-of-%s.%s";
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

    public ResourceId forBucketShard(int bucketNumber, int numBuckets, int shardNumber, int numShards) {
      String prefix = "";
      if (doTimestampFiles) {
        prefix += Instant.now().toString(DateTimeFormat.forPattern(TIMESTAMP_TEMPLATE));
      }

      return filenamePrefix.resolve(
          prefix + String.format(BUCKET_TEMPLATE, bucketNumber, numBuckets, shardNumber, numShards, fileNameSuffix),
          StandardResolveOptions.RESOLVE_FILE
      );
    }

    public ResourceId forMetadata() {
      return filenamePrefix.resolve(METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
    }
  }
}
