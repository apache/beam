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
package org.apache.beam.sdk.extensions.gcp.util;

import com.google.api.client.util.DateTime;
import com.google.api.gax.paging.Page;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.BucketTargetOption;
import com.google.cloud.storage.Storage.PredefinedAcl;
import com.google.cloud.storage.StorageClass;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.BlobResult;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.MissingStrategy;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtilV2.OverwriteStrategy;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GcsUtil.class);

  /**
   * Namespace for every GCS metric. The namespace is dropped when Dataflow exports counters to
   * Cloud Monitoring, so the layer is carried by the metric name instead: {@code gcs_http_*} for
   * transport-level counters and {@code gcs_op_*} for operation-level ones.
   */
  public static final String METRIC_NAMESPACE = "Gcs";

  @VisibleForTesting GcsUtilV1 delegate;
  @VisibleForTesting @Nullable GcsUtilV2 delegateV2;

  /**
   * @deprecated no {@link GcsUtil} API accepts this type, so an instance cannot be used for
   *     anything. GCS counters are configured from {@link
   *     org.apache.beam.sdk.extensions.gcp.options.GcsOptions} when the {@link GcsUtil} is
   *     constructed. Scheduled for removal.
   */
  @Deprecated
  public static class GcsCountersOptions {
    final GcsUtilV1.GcsCountersOptions delegate;

    private GcsCountersOptions(GcsUtilV1.GcsCountersOptions delegate) {
      this.delegate = delegate;
    }

    public @Nullable String getReadCounterPrefix() {
      return delegate.getReadCounterPrefix();
    }

    public @Nullable String getWriteCounterPrefix() {
      return delegate.getWriteCounterPrefix();
    }

    public boolean hasAnyPrefix() {
      return delegate.hasAnyPrefix();
    }

    public static GcsCountersOptions create(
        @Nullable String readCounterPrefix, @Nullable String writeCounterPrefix) {
      return new GcsCountersOptions(
          GcsUtilV1.GcsCountersOptions.create(readCounterPrefix, writeCounterPrefix));
    }
  }

  public static class GcsUtilFactory implements DefaultValueFactory<GcsUtil> {
    @Override
    public GcsUtil create(PipelineOptions options) {
      return new GcsUtil(options);
    }
  }

  /**
   * @deprecated use {@link GcsPath#getNonWildcardPrefix(String)} instead.
   */
  @Deprecated
  public static String getNonWildcardPrefix(String globExp) {
    return GcsPath.getNonWildcardPrefix(globExp);
  }

  /**
   * @deprecated use {@link GcsPath#isWildcard(GcsPath)} instead.
   */
  @Deprecated
  public static boolean isWildcard(GcsPath spec) {
    return GcsPath.isWildcard(spec);
  }

  GcsUtil(PipelineOptions options) {
    this.delegate = new GcsUtilV1.GcsUtilFactory().create(options);
    if (ExperimentalOptions.hasExperiment(options, "use_gcsutil_v2")) {
      this.delegateV2 = new GcsUtilV2.GcsUtilFactory().create(options);
      // INFO only for V2, which is opt-in. V1 is still the default for every pipeline,
      // so logging it at INFO would be noise.
      LOG.info("Using GcsUtilV2 (java-storage) for GCS operations.");
    } else {
      this.delegateV2 = null;
      LOG.debug("Using GcsUtilV1 (gcsio) for GCS operations.");
    }
  }

  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.expand(gcsPattern);
    }
    return delegate.expand(gcsPattern);
  }

  public long fileSize(GcsPath path) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.fileSize(path);
    }
    return delegate.fileSize(path);
  }

  /**
   * @deprecated use {@link #getBlob(GcsPath, BlobGetOption...)}.
   */
  @Deprecated
  public StorageObject getObject(GcsPath gcsPath) throws IOException {
    if (delegateV2 != null) {
      return toStorageObject(delegateV2.getBlob(gcsPath));
    }
    return delegate.getObject(gcsPath);
  }

  public Blob getBlob(GcsPath gcsPath, BlobGetOption... options) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.getBlob(gcsPath, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  /**
   * @deprecated use {@link #getBlobs(Iterable, BlobGetOption...)}.
   */
  @Deprecated
  public List<StorageObjectOrIOException> getObjects(List<GcsPath> gcsPaths) throws IOException {
    if (delegateV2 != null) {
      List<StorageObjectOrIOException> results = new ArrayList<>();
      for (BlobResult blobResult : delegateV2.getBlobs(gcsPaths)) {
        Blob blob = blobResult.blob();
        IOException ioException = blobResult.ioException();
        if (blob != null) {
          results.add(StorageObjectOrIOException.create(toStorageObject(blob)));
        } else if (ioException != null) {
          results.add(StorageObjectOrIOException.create(ioException));
        } else {
          throw new IOException("Invalid blob result: it holds neither a blob nor an error.");
        }
      }
      return results;
    }
    List<GcsUtilV1.StorageObjectOrIOException> legacy = delegate.getObjects(gcsPaths);
    return legacy.stream()
        .map(StorageObjectOrIOException::fromLegacy)
        .collect(java.util.stream.Collectors.toList());
  }

  public List<BlobResult> getBlobs(Iterable<GcsPath> gcsPaths, BlobGetOption... options)
      throws IOException {
    if (delegateV2 != null) {
      return delegateV2.getBlobs(gcsPaths, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  /**
   * @deprecated use {@link #listBlobs(String, String, String, BlobListOption...)}.
   */
  @Deprecated
  public Objects listObjects(String bucket, String prefix, @Nullable String pageToken)
      throws IOException {
    if (delegateV2 != null) {
      return toObjects(delegateV2.listBlobs(bucket, prefix, pageToken));
    }
    return delegate.listObjects(bucket, prefix, pageToken);
  }

  /**
   * @deprecated use {@link #listBlobs(String, String, String, String, BlobListOption...)}.
   */
  @Deprecated
  public Objects listObjects(
      String bucket, String prefix, @Nullable String pageToken, @Nullable String delimiter)
      throws IOException {
    if (delegateV2 != null) {
      return toObjects(delegateV2.listBlobs(bucket, prefix, pageToken, delimiter));
    }
    return delegate.listObjects(bucket, prefix, pageToken, delimiter);
  }

  public Page<Blob> listBlobs(
      String bucket, String prefix, @Nullable String pageToken, BlobListOption... options)
      throws IOException {
    if (delegateV2 != null) {
      return delegateV2.listBlobs(bucket, prefix, pageToken, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  public Page<Blob> listBlobs(
      String bucket,
      String prefix,
      @Nullable String pageToken,
      @Nullable String delimiter,
      BlobListOption... options)
      throws IOException {
    if (delegateV2 != null) {
      return delegateV2.listBlobs(bucket, prefix, pageToken, delimiter, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  public SeekableByteChannel open(GcsPath path) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.open(path);
    }
    return delegate.open(path);
  }

  public SeekableByteChannel openV2(GcsPath path, BlobSourceOption... options) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.open(path, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  /**
   * @deprecated Use {@link #create(GcsPath, CreateOptions)} instead.
   */
  @Deprecated
  public WritableByteChannel create(GcsPath path, String type) throws IOException {
    return delegate.create(path, type);
  }

  /**
   * @deprecated Use {@link #create(GcsPath, CreateOptions)} instead.
   */
  @Deprecated
  public WritableByteChannel create(GcsPath path, String type, Integer uploadBufferSizeBytes)
      throws IOException {
    return delegate.create(path, type, uploadBufferSizeBytes);
  }

  public static class CreateOptions {
    final GcsUtilV1.CreateOptions delegate;

    private CreateOptions(GcsUtilV1.CreateOptions delegate) {
      this.delegate = delegate;
    }

    public boolean getExpectFileToNotExist() {
      return delegate.getExpectFileToNotExist();
    }

    public @Nullable Integer getUploadBufferSizeBytes() {
      return delegate.getUploadBufferSizeBytes();
    }

    public @Nullable String getContentType() {
      return delegate.getContentType();
    }

    public static Builder builder() {
      return new Builder(GcsUtilV1.CreateOptions.builder());
    }

    public static class Builder {
      private final GcsUtilV1.CreateOptions.Builder delegateBuilder;

      private Builder(GcsUtilV1.CreateOptions.Builder delegateBuilder) {
        this.delegateBuilder = delegateBuilder;
      }

      public Builder setContentType(String value) {
        delegateBuilder.setContentType(value);
        return this;
      }

      public Builder setUploadBufferSizeBytes(int value) {
        delegateBuilder.setUploadBufferSizeBytes(value);
        return this;
      }

      public Builder setExpectFileToNotExist(boolean value) {
        delegateBuilder.setExpectFileToNotExist(value);
        return this;
      }

      public CreateOptions build() {
        return new CreateOptions(delegateBuilder.build());
      }
    }
  }

  public WritableByteChannel create(GcsPath path, CreateOptions options) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.create(path, options.delegate);
    }
    return delegate.create(path, options.delegate);
  }

  public WritableByteChannel createV2(
      GcsPath path, CreateOptions options, BlobWriteOption... writeOptions) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.create(path, options.delegate, writeOptions);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  public void verifyBucketAccessible(GcsPath path) throws IOException {
    if (delegateV2 != null) {
      delegateV2.verifyBucketAccessible(path);
      return;
    }
    delegate.verifyBucketAccessible(path);
  }

  public boolean bucketAccessible(GcsPath path) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.bucketAccessible(path);
    }
    return delegate.bucketAccessible(path);
  }

  public long bucketOwner(GcsPath path) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.bucketProject(path);
    }
    return delegate.bucketOwner(path);
  }

  /**
   * @deprecated use {@link #createBucket(BucketInfo, BucketTargetOption...)}.
   */
  @Deprecated
  public void createBucket(String projectId, Bucket bucket) throws IOException {
    if (delegateV2 != null) {
      // GcsUtilV1 always creates buckets with projectPrivate ACLs, which java-storage does not do
      // on its own, so they have to be requested explicitly to keep the same access.
      delegateV2.createBucket(
          projectId,
          toBucketInfo(bucket),
          BucketTargetOption.predefinedAcl(PredefinedAcl.PROJECT_PRIVATE),
          BucketTargetOption.predefinedDefaultObjectAcl(PredefinedAcl.PROJECT_PRIVATE));
      return;
    }
    delegate.createBucket(projectId, bucket);
  }

  public void createBucket(BucketInfo bucketInfo, BucketTargetOption... options)
      throws IOException {
    if (delegateV2 != null) {
      delegateV2.createBucket(bucketInfo, options);
    } else {
      throw new IOException("GcsUtil V2 not initialized.");
    }
  }

  /**
   * @deprecated use {@link #getBucketWithOptions(GcsPath, BucketGetOption...)} .
   */
  @Deprecated
  public @Nullable Bucket getBucket(GcsPath path) throws IOException {
    return delegate.getBucket(path);
  }

  public com.google.cloud.storage.@Nullable Bucket getBucketWithOptions(
      GcsPath path, BucketGetOption... options) throws IOException {
    if (delegateV2 != null) {
      return delegateV2.getBucket(path, options);
    }
    throw new IOException("GcsUtil V2 not initialized.");
  }

  /**
   * @deprecated use {@link #removeBucket(BucketInfo)}.
   */
  @Deprecated
  public void removeBucket(Bucket bucket) throws IOException {
    if (delegateV2 != null) {
      delegateV2.removeBucket(toBucketInfo(bucket));
      return;
    }
    delegate.removeBucket(bucket);
  }

  public void removeBucket(BucketInfo bucketInfo) throws IOException {
    if (delegateV2 != null) {
      delegateV2.removeBucket(bucketInfo);
    } else {
      throw new IOException("GcsUtil V2 not initialized.");
    }
  }

  public void copy(Iterable<String> srcFilenames, Iterable<String> destFilenames)
      throws IOException {
    if (delegateV2 != null) {
      // GcsUtilV1 issues a rewrite without any destination precondition, so ALWAYS_OVERWRITE is
      // the strategy that preserves its behavior. The strategies that inspect the destination
      // would also cost an extra GET per file.
      delegateV2.copy(
          toGcsPaths(srcFilenames), toGcsPaths(destFilenames), OverwriteStrategy.ALWAYS_OVERWRITE);
      return;
    }
    delegate.copy(srcFilenames, destFilenames);
  }

  /** experimental api. */
  public void copyV2(Iterable<GcsPath> srcPaths, Iterable<GcsPath> dstPaths) throws IOException {
    copy(srcPaths, dstPaths, OverwriteStrategy.SAFE_OVERWRITE);
  }

  /** experimental api. */
  public void copy(
      Iterable<GcsPath> srcPaths, Iterable<GcsPath> dstPaths, OverwriteStrategy strategy)
      throws IOException {
    if (delegateV2 != null) {
      delegateV2.copy(srcPaths, dstPaths, strategy);
    } else {
      throw new IOException("GcsUtil V2 not initialized.");
    }
  }

  public void rename(
      Iterable<String> srcFilenames, Iterable<String> destFilenames, MoveOptions... moveOptions)
      throws IOException {
    GcsUtilV2 v2 = delegateV2;
    if (v2 != null) {
      Set<MoveOptions> moveOptionSet = Sets.newHashSet(moveOptions);
      // Note this differs from renameV2, which defaults to SAFE_OVERWRITE. GcsUtilV1 rewrites
      // without a destination precondition, so ALWAYS_OVERWRITE is the behavior preserving choice.
      v2.move(
          toGcsPaths(srcFilenames),
          toGcsPaths(destFilenames),
          moveOptionSet.contains(StandardMoveOptions.IGNORE_MISSING_FILES)
              ? MissingStrategy.SKIP_IF_MISSING
              : MissingStrategy.FAIL_IF_MISSING,
          moveOptionSet.contains(StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS)
              ? OverwriteStrategy.SKIP_IF_EXISTS
              : OverwriteStrategy.ALWAYS_OVERWRITE);
      return;
    }
    delegate.rename(srcFilenames, destFilenames, moveOptions);
  }

  /** experimental api. */
  public void renameV2(
      Iterable<GcsPath> srcPaths, Iterable<GcsPath> dstPaths, MoveOptions... moveOptions)
      throws IOException {
    Set<MoveOptions> moveOptionSet = Sets.newHashSet(moveOptions);
    final MissingStrategy srcMissing;
    final OverwriteStrategy dstOverwrite;

    if (moveOptionSet.contains(StandardMoveOptions.IGNORE_MISSING_FILES)) {
      srcMissing = MissingStrategy.SKIP_IF_MISSING;
    } else {
      srcMissing = MissingStrategy.FAIL_IF_MISSING;
    }

    if (moveOptionSet.contains(StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS)) {
      dstOverwrite = OverwriteStrategy.SKIP_IF_EXISTS;
    } else {
      dstOverwrite = OverwriteStrategy.SAFE_OVERWRITE;
    }

    rename(srcPaths, dstPaths, srcMissing, dstOverwrite);
  }

  /** experimental api. */
  public void rename(
      Iterable<GcsPath> srcPaths,
      Iterable<GcsPath> dstPaths,
      MissingStrategy srcMissing,
      OverwriteStrategy dstOverwrite)
      throws IOException {
    if (delegateV2 != null) {
      delegateV2.move(srcPaths, dstPaths, srcMissing, dstOverwrite);
    } else {
      throw new IOException("GcsUtil V2 not initialized.");
    }
  }

  public void remove(Collection<String> filenames) throws IOException {
    if (delegateV2 != null) {
      // GcsUtilV1 ignores a 404 on delete, which is SKIP_IF_MISSING.
      delegateV2.remove(toGcsPaths(filenames), MissingStrategy.SKIP_IF_MISSING);
      return;
    }
    delegate.remove(filenames);
  }

  /** experimental api. */
  public void removeV2(Iterable<GcsPath> paths) throws IOException {
    remove(paths, MissingStrategy.SKIP_IF_MISSING);
  }

  /** experimental api. */
  public void remove(Iterable<GcsPath> paths, MissingStrategy strategy) throws IOException {
    if (delegateV2 != null) {
      delegateV2.remove(paths, strategy);
    } else {
      throw new IOException("GcsUtil V2 not initialized.");
    }
  }

  private static List<GcsPath> toGcsPaths(Iterable<String> filenames) {
    List<GcsPath> paths = new ArrayList<>();
    for (String filename : filenames) {
      paths.add(GcsPath.fromUri(filename));
    }
    return paths;
  }

  /**
   * Converts a JSON API {@link Bucket} into the java-storage {@link BucketInfo} model.
   *
   * <p>Only the properties that callers of the deprecated {@link #createBucket(String, Bucket)} set
   * are carried over. Like {@link #toStorageObject}, this is expected to go away with the
   * deprecated methods it serves.
   */
  private static BucketInfo toBucketInfo(Bucket bucket) {
    BucketInfo.Builder builder = BucketInfo.newBuilder(bucket.getName());
    if (bucket.getLocation() != null) {
      builder.setLocation(bucket.getLocation());
    }
    if (bucket.getStorageClass() != null) {
      builder.setStorageClass(StorageClass.valueOf(bucket.getStorageClass()));
    }
    Bucket.SoftDeletePolicy softDeletePolicy = bucket.getSoftDeletePolicy();
    if (softDeletePolicy != null && softDeletePolicy.getRetentionDurationSeconds() != null) {
      builder.setSoftDeletePolicy(
          BucketInfo.SoftDeletePolicy.newBuilder()
              .setRetentionDuration(
                  Duration.ofSeconds(softDeletePolicy.getRetentionDurationSeconds()))
              .build());
    }
    return builder.build();
  }

  /**
   * Converts a java-storage {@link Blob} back into the JSON API {@link StorageObject} model.
   *
   * <p>This lets the deprecated, legacy typed methods of this class be served by {@link GcsUtilV2}
   * without their callers having to change. It is expected to go away once those methods do.
   */
  private static StorageObject toStorageObject(Blob blob) {
    StorageObject storageObject =
        new StorageObject()
            .setBucket(blob.getBucket())
            .setName(blob.getName())
            .setGeneration(blob.getGeneration())
            .setMetageneration(blob.getMetageneration())
            .setContentType(blob.getContentType())
            .setContentEncoding(blob.getContentEncoding())
            .setMd5Hash(blob.getMd5())
            .setCrc32c(blob.getCrc32c())
            .setEtag(blob.getEtag());
    Long size = blob.getSize();
    if (size != null) {
      storageObject.setSize(BigInteger.valueOf(size));
    }
    OffsetDateTime updated = blob.getUpdateTimeOffsetDateTime();
    if (updated != null) {
      storageObject.setUpdated(new DateTime(updated.toInstant().toEpochMilli()));
    }
    OffsetDateTime created = blob.getCreateTimeOffsetDateTime();
    if (created != null) {
      storageObject.setTimeCreated(new DateTime(created.toInstant().toEpochMilli()));
    }
    return storageObject;
  }

  /** Converts a single page of java-storage {@link Blob}s into the JSON API {@link Objects}. */
  private static Objects toObjects(Page<Blob> page) {
    List<StorageObject> items = new ArrayList<>();
    List<String> prefixes = new ArrayList<>();
    for (Blob blob : page.getValues()) {
      // A delimited listing reports each common prefix as a directory placeholder.
      if (blob.isDirectory()) {
        prefixes.add(blob.getName());
      } else {
        items.add(toStorageObject(blob));
      }
    }
    Objects objects = new Objects();
    // Leave items and prefixes null when empty, as the JSON API does, so that callers looping on
    // getItems() != null keep terminating.
    if (!items.isEmpty()) {
      objects.setItems(items);
    }
    if (!prefixes.isEmpty()) {
      objects.setPrefixes(prefixes);
    }
    // Page.getNextPageToken() may be an empty string rather than null on the last page.
    String nextPageToken = page.hasNextPage() ? page.getNextPageToken() : null;
    if (nextPageToken != null) {
      objects.setNextPageToken(nextPageToken);
    }
    return objects;
  }

  @SuppressFBWarnings("NM_CLASS_NOT_EXCEPTION")
  public static class StorageObjectOrIOException {
    final GcsUtilV1.StorageObjectOrIOException delegate;

    private StorageObjectOrIOException(GcsUtilV1.StorageObjectOrIOException delegate) {
      this.delegate = delegate;
    }

    public static StorageObjectOrIOException create(StorageObject storageObject) {
      return new StorageObjectOrIOException(
          GcsUtilV1.StorageObjectOrIOException.create(storageObject));
    }

    public static StorageObjectOrIOException create(IOException ioException) {
      return new StorageObjectOrIOException(
          GcsUtilV1.StorageObjectOrIOException.create(ioException));
    }

    static StorageObjectOrIOException fromLegacy(GcsUtilV1.StorageObjectOrIOException legacy) {
      return new StorageObjectOrIOException(legacy);
    }

    public @Nullable StorageObject storageObject() {
      return delegate.storageObject();
    }

    public @Nullable IOException ioException() {
      return delegate.ioException();
    }
  }
}
