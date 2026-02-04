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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GcsUtil {
  @VisibleForTesting GcsUtilV1 delegate;
  @VisibleForTesting @Nullable GcsUtilV2 delegateV2;

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
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      Storage.Builder storageBuilder = Transport.newStorageClient(gcsOptions);
      return new GcsUtil(
          storageBuilder.build(),
          storageBuilder.getHttpRequestInitializer(),
          gcsOptions.getExecutorService(),
          ExperimentalOptions.hasExperiment(options, "use_grpc_for_gcs"),
          gcsOptions.getGcpCredential(),
          gcsOptions.getGcsUploadBufferSizeBytes(),
          gcsOptions.getGcsRewriteDataOpBatchLimit(),
          GcsCountersOptions.create(
              gcsOptions.getEnableBucketReadMetricCounter()
                  ? gcsOptions.getGcsReadCounterPrefix()
                  : null,
              gcsOptions.getEnableBucketWriteMetricCounter()
                  ? gcsOptions.getGcsWriteCounterPrefix()
                  : null),
          gcsOptions,
          ExperimentalOptions.hasExperiment(options, "use_gcsutil_v2"));
    }
  }

  public static String getNonWildcardPrefix(String globExp) {
    return GcsUtilV1.getNonWildcardPrefix(globExp);
  }

  public static boolean isWildcard(GcsPath spec) {
    return GcsUtilV1.isWildcard(spec);
  }

  @VisibleForTesting
  GcsUtil(
      Storage storageClient,
      HttpRequestInitializer httpRequestInitializer,
      ExecutorService executorService,
      Boolean shouldUseGrpc,
      Credentials credentials,
      @Nullable Integer uploadBufferSizeBytes,
      @Nullable Integer rewriteDataOpBatchLimit,
      GcsCountersOptions gcsCountersOptions,
      GcsOptions gcsOptions) {
    this.delegate =
        new GcsUtilV1(
            storageClient,
            httpRequestInitializer,
            executorService,
            shouldUseGrpc,
            credentials,
            uploadBufferSizeBytes,
            rewriteDataOpBatchLimit,
            gcsCountersOptions.delegate,
            gcsOptions);
    this.delegateV2 = null;
  }

  @VisibleForTesting
  GcsUtil(
      Storage storageClient,
      HttpRequestInitializer httpRequestInitializer,
      ExecutorService executorService,
      Boolean shouldUseGrpc,
      Credentials credentials,
      @Nullable Integer uploadBufferSizeBytes,
      @Nullable Integer rewriteDataOpBatchLimit,
      GcsCountersOptions gcsCountersOptions,
      GcsOptions gcsOptions,
      Boolean shouldUseV2) {
    this.delegate =
        new GcsUtilV1(
            storageClient,
            httpRequestInitializer,
            executorService,
            shouldUseGrpc,
            credentials,
            uploadBufferSizeBytes,
            rewriteDataOpBatchLimit,
            gcsCountersOptions.delegate,
            gcsOptions);

    if (shouldUseV2) {
      this.delegateV2 = new GcsUtilV2(gcsOptions);
    }
  }

  protected void setStorageClient(Storage storageClient) {
    delegate.setStorageClient(storageClient);
  }

  protected void setBatchRequestSupplier(Supplier<GcsUtilV1.BatchInterface> supplier) {
    delegate.setBatchRequestSupplier(supplier);
  }

  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    return delegate.expand(gcsPattern);
  }

  @VisibleForTesting
  @Nullable
  Integer getUploadBufferSizeBytes() {
    return delegate.getUploadBufferSizeBytes();
  }

  public long fileSize(GcsPath path) throws IOException {
    if (delegateV2 != null) return delegateV2.fileSize(path);
    return delegate.fileSize(path);
  }

  /** @deprecated use {@link #getBlob(GcsPath)}. */
  @Deprecated
  public StorageObject getObject(GcsPath gcsPath) throws IOException {
    return delegate.getObject(gcsPath);
  }

  /** @deprecated use {@link #getBlob(GcsPath)}. */
  @Deprecated
  @VisibleForTesting
  StorageObject getObject(GcsPath gcsPath, BackOff backoff, Sleeper sleeper) throws IOException {
    return delegate.getObject(gcsPath, backoff, sleeper);
  }

  public Blob getBlob(GcsPath gcsPath) throws IOException {
    if (delegateV2 != null) return delegateV2.getBlob(gcsPath);
    throw new IOException("GcsUtil2 not initialized.");
  }

  public List<StorageObjectOrIOException> getObjects(List<GcsPath> gcsPaths) throws IOException {
    List<GcsUtilV1.StorageObjectOrIOException> legacy = delegate.getObjects(gcsPaths);
    return legacy.stream()
        .map(StorageObjectOrIOException::fromLegacy)
        .collect(java.util.stream.Collectors.toList());
  }

  public Objects listObjects(String bucket, String prefix, @Nullable String pageToken)
      throws IOException {
    return delegate.listObjects(bucket, prefix, pageToken);
  }

  public Objects listObjects(
      String bucket, String prefix, @Nullable String pageToken, @Nullable String delimiter)
      throws IOException {
    return delegate.listObjects(bucket, prefix, pageToken, delimiter);
  }

  @VisibleForTesting
  List<Long> fileSizes(List<GcsPath> paths) throws IOException {
    return delegate.fileSizes(paths);
  }

  public SeekableByteChannel open(GcsPath path) throws IOException {
    return delegate.open(path);
  }

  /** @deprecated Use {@link #create(GcsPath, CreateOptions)} instead. */
  @Deprecated
  public WritableByteChannel create(GcsPath path, String type) throws IOException {
    return delegate.create(path, type);
  }

  /** @deprecated Use {@link #create(GcsPath, CreateOptions)} instead. */
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
    return delegate.create(path, options.delegate);
  }

  public void verifyBucketAccessible(GcsPath path) throws IOException {
    delegate.verifyBucketAccessible(path);
  }

  public boolean bucketAccessible(GcsPath path) throws IOException {
    return delegate.bucketAccessible(path);
  }

  public long bucketOwner(GcsPath path) throws IOException {
    return delegate.bucketOwner(path);
  }

  public void createBucket(String projectId, Bucket bucket) throws IOException {
    delegate.createBucket(projectId, bucket);
  }

  public @Nullable Bucket getBucket(GcsPath path) throws IOException {
    return delegate.getBucket(path);
  }

  public void removeBucket(Bucket bucket) throws IOException {
    delegate.removeBucket(bucket);
  }

  @VisibleForTesting
  boolean bucketAccessible(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    return delegate.bucketAccessible(path, backoff, sleeper);
  }

  @VisibleForTesting
  void verifyBucketAccessible(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    delegate.verifyBucketAccessible(path, backoff, sleeper);
  }

  @VisibleForTesting
  @Nullable
  Bucket getBucket(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    return delegate.getBucket(path, backoff, sleeper);
  }

  @VisibleForTesting
  void createBucket(String projectId, Bucket bucket, BackOff backoff, Sleeper sleeper)
      throws IOException {
    delegate.createBucket(projectId, bucket, backoff, sleeper);
  }

  @VisibleForTesting
  void removeBucket(Bucket bucket, BackOff backoff, Sleeper sleeper) throws IOException {
    delegate.removeBucket(bucket, backoff, sleeper);
  }

  @VisibleForTesting
  List<GcsUtilV1.BatchInterface> makeGetBatches(
      Collection<GcsPath> paths, List<StorageObjectOrIOException[]> results) throws IOException {
    List<GcsUtilV1.StorageObjectOrIOException[]> legacyResults = new java.util.ArrayList<>();
    List<GcsUtilV1.BatchInterface> legacyBatch = delegate.makeGetBatches(paths, legacyResults);

    for (GcsUtilV1.StorageObjectOrIOException[] legacyResult : legacyResults) {
      StorageObjectOrIOException[] result = new StorageObjectOrIOException[legacyResult.length];
      for (int i = 0; i < legacyResult.length; ++i) {
        result[i] = StorageObjectOrIOException.fromLegacy(legacyResult[i]);
      }
      results.add(result);
    }

    return legacyBatch;
  }

  public void copy(Iterable<String> srcFilenames, Iterable<String> destFilenames)
      throws IOException {
    delegate.copy(srcFilenames, destFilenames);
  }

  public void rename(
      Iterable<String> srcFilenames, Iterable<String> destFilenames, MoveOptions... moveOptions)
      throws IOException {
    delegate.rename(srcFilenames, destFilenames, moveOptions);
  }

  @VisibleForTesting
  @SuppressWarnings("JdkObsolete") // for LinkedList
  java.util.LinkedList<GcsUtilV1.RewriteOp> makeRewriteOps(
      Iterable<String> srcFilenames,
      Iterable<String> destFilenames,
      boolean deleteSource,
      boolean ignoreMissingSource,
      boolean ignoreExistingDest)
      throws IOException {
    return delegate.makeRewriteOps(
        srcFilenames, destFilenames, deleteSource, ignoreMissingSource, ignoreExistingDest);
  }

  @VisibleForTesting
  @SuppressWarnings("JdkObsolete") // for LinkedList
  List<GcsUtilV1.BatchInterface> makeRewriteBatches(
      java.util.LinkedList<GcsUtilV1.RewriteOp> rewrites) throws IOException {
    return delegate.makeRewriteBatches(rewrites);
  }

  @VisibleForTesting
  List<GcsUtilV1.BatchInterface> makeRemoveBatches(Collection<String> filenames)
      throws IOException {
    return delegate.makeRemoveBatches(filenames);
  }

  public void remove(Collection<String> filenames) throws IOException {
    delegate.remove(filenames);
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
