package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.ResourceId;

// @Todo = everything. This is just a placeholder for the kind of functionality we need
public abstract class BucketingMetadata<SortingKeyT, ValueT> {
  // Instead of coding to bytes this file should probably be human readable?
  // Maybe include an OutputFileHints variable in this class for file naming convention
  public abstract Coder<BucketingMetadata<SortingKeyT, ValueT>> getCoder();

  // @todo use an actual hashing fn and modulo desired number of buckets
  public abstract Integer assignBucket(ValueT value);

  public abstract SortingKeyT extractSortingKey(ValueT value);

  public abstract Coder<SortingKeyT> getSortingKeyCoder();

  public abstract Integer getNumBuckets();

  public abstract ResourceId getMetadataResource();
}
