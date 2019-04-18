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
  private final Integer numBuckets;

  public SMBFilenamePolicy(
      ResourceId destinationPrefix,
      String fileNameSuffix,
      ResourceId tempDirectory,
      Integer numBuckets
  ) {
    this.filenamePrefix = destinationPrefix;
    this.fileNameSuffix = fileNameSuffix;
    this.tempDirectory = tempDirectory;
    this.numBuckets = numBuckets;
  }

  public FileAssignment forDestination() {
    return new FileAssignment(filenamePrefix, fileNameSuffix, numBuckets);
  }

  public FileAssignment forTempFiles() {
    return new FileAssignment(
        tempDirectory.resolve(
          String.format(
              BEAM_TEMPDIR_PATTERN,
              Instant.now().toString(DateTimeFormat.forPattern(TEMPDIR_TIMESTAMP))
          ), StandardResolveOptions.RESOLVE_DIRECTORY),
        fileNameSuffix,
        numBuckets
    );
  }

  static class FileAssignment implements Serializable {
    private static final String BUCKET_TEMPLATE = "bucket-%d-of-%d-shard-%d-of-%s.%s";
    private static final String METADATA_FILENAME = "metadata.json";

    private final ResourceId filenamePrefix;
    private final String fileNameSuffix;
    private final Integer numBuckets;

    FileAssignment(ResourceId filenamePrefix, String fileNameSuffix, Integer numBuckets) {
      this.filenamePrefix = filenamePrefix;
      this.fileNameSuffix = fileNameSuffix;
      this.numBuckets = numBuckets;
    }

    public ResourceId forBucketShard(int bucketNumber, int shardNumber, int numShards) {
      return filenamePrefix.resolve(
          String.format(BUCKET_TEMPLATE, bucketNumber, numBuckets, shardNumber, numShards, fileNameSuffix),
          StandardResolveOptions.RESOLVE_FILE
      );
    }

    public ResourceId forMetadata() {
      return filenamePrefix.resolve(METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
    }
  }
}
