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

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GcsUtil {
  @VisibleForTesting GcsUtilLegacy delegate;

  public static class GcsCountersOptions {
    final GcsUtilLegacy.GcsCountersOptions delegate;

    private GcsCountersOptions(GcsUtilLegacy.GcsCountersOptions delegate) {
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
          GcsUtilLegacy.GcsCountersOptions.create(readCounterPrefix, writeCounterPrefix));
    }
  }

  public static class GcsReadOptionsFactory
      implements DefaultValueFactory<GoogleCloudStorageReadOptions> {
    @Override
    public GoogleCloudStorageReadOptions create(PipelineOptions options) {
      return new GcsUtilLegacy.GcsReadOptionsFactory().create(options);
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
          org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment(
              options, "use_grpc_for_gcs"),
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
          gcsOptions.getGoogleCloudStorageReadOptions());
    }

    public static GcsUtil create(
        PipelineOptions options,
        Storage storageClient,
        HttpRequestInitializer httpRequestInitializer,
        ExecutorService executorService,
        Credentials credentials,
        @Nullable Integer uploadBufferSizeBytes,
        GcsCountersOptions gcsCountersOptions,
        GoogleCloudStorageReadOptions gcsReadOptions) {
      return new GcsUtil(
          storageClient,
          httpRequestInitializer,
          executorService,
          org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment(
              options, "use_grpc_for_gcs"),
          credentials,
          uploadBufferSizeBytes,
          null,
          gcsCountersOptions,
          gcsReadOptions);
    }
  }

  public static String getNonWildcardPrefix(String globExp) {
    return GcsUtilLegacy.getNonWildcardPrefix(globExp);
  }

  public static boolean isWildcard(GcsPath spec) {
    return GcsUtilLegacy.isWildcard(spec);
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
      GoogleCloudStorageReadOptions gcsReadOptions) {
    this.delegate =
        new GcsUtilLegacy(
            storageClient,
            httpRequestInitializer,
            executorService,
            shouldUseGrpc,
            credentials,
            uploadBufferSizeBytes,
            rewriteDataOpBatchLimit,
            gcsCountersOptions.delegate,
            gcsReadOptions);
  }

  protected void setStorageClient(Storage storageClient) {
    delegate.setStorageClient(storageClient);
  }

  protected void setBatchRequestSupplier(Supplier<BatchInterface> supplier) {
    delegate.setBatchRequestSupplier(() -> new LegacyBatchAdapter(supplier.get()));
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
    return delegate.fileSize(path);
  }

  public StorageObject getObject(GcsPath gcsPath) throws IOException {
    return delegate.getObject(gcsPath);
  }

  @VisibleForTesting
  StorageObject getObject(GcsPath gcsPath, BackOff backoff, Sleeper sleeper) throws IOException {
    return delegate.getObject(gcsPath, backoff, sleeper);
  }

  public List<StorageObjectOrIOException> getObjects(List<GcsPath> gcsPaths) throws IOException {
    List<GcsUtilLegacy.StorageObjectOrIOException> legacy = delegate.getObjects(gcsPaths);
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

  @VisibleForTesting
  SeekableByteChannel open(GcsPath path, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    return delegate.open(path, readOptions);
  }

  @Deprecated
  public WritableByteChannel create(GcsPath path, String type) throws IOException {
    return delegate.create(path, type);
  }

  @Deprecated
  public WritableByteChannel create(GcsPath path, String type, Integer uploadBufferSizeBytes)
      throws IOException {
    return delegate.create(path, type, uploadBufferSizeBytes);
  }

  public static class CreateOptions {
    final GcsUtilLegacy.CreateOptions delegate;

    private CreateOptions(GcsUtilLegacy.CreateOptions delegate) {
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
      return new Builder(GcsUtilLegacy.CreateOptions.builder());
    }

    public static class Builder {
      private final GcsUtilLegacy.CreateOptions.Builder delegateBuilder;

      private Builder(GcsUtilLegacy.CreateOptions.Builder delegateBuilder) {
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
  List<BatchInterface> makeGetBatches(
      Collection<GcsPath> paths, List<StorageObjectOrIOException[]> results) throws IOException {
    List<GcsUtilLegacy.StorageObjectOrIOException[]> legacyResults = new java.util.ArrayList<>();
    List<GcsUtilLegacy.BatchInterface> legacyBatches =
        delegate.makeGetBatches(paths, legacyResults);
    for (int i = 0; i < legacyResults.size(); i++) {
      results.add(new StorageObjectOrIOException[1]);
    }
    List<BatchInterface> mapped = new java.util.ArrayList<>();
    for (GcsUtilLegacy.BatchInterface b : legacyBatches) {
      mapped.add(new BatchAdapter(b));
    }
    return mapped;
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
  @SuppressWarnings("JdkObsolete")
  java.util.LinkedList<RewriteOp> makeRewriteOps(
      Iterable<String> srcFilenames,
      Iterable<String> destFilenames,
      boolean deleteSource,
      boolean ignoreMissingSource,
      boolean ignoreExistingDest)
      throws IOException {
    java.util.LinkedList<GcsUtilLegacy.RewriteOp> legacy =
        delegate.makeRewriteOps(
            srcFilenames, destFilenames, deleteSource, ignoreMissingSource, ignoreExistingDest);
    java.util.LinkedList<RewriteOp> mapped = new java.util.LinkedList<>();
    for (GcsUtilLegacy.RewriteOp op : legacy) {
      mapped.add(new RewriteOp(op));
    }
    return mapped;
  }

  @VisibleForTesting
  @SuppressWarnings("JdkObsolete")
  List<BatchInterface> makeRewriteBatches(java.util.LinkedList<RewriteOp> rewrites)
      throws IOException {
    java.util.LinkedList<GcsUtilLegacy.RewriteOp> legacy = new java.util.LinkedList<>();
    for (RewriteOp op : rewrites) {
      legacy.add(op.delegate);
    }
    List<GcsUtilLegacy.BatchInterface> legacyBatches = delegate.makeRewriteBatches(legacy);
    List<BatchInterface> mapped = new java.util.ArrayList<>();
    for (GcsUtilLegacy.BatchInterface b : legacyBatches) {
      mapped.add(new BatchAdapter(b));
    }
    return mapped;
  }

  @VisibleForTesting
  List<BatchInterface> makeRemoveBatches(Collection<String> filenames) throws IOException {
    List<GcsUtilLegacy.BatchInterface> legacy = delegate.makeRemoveBatches(filenames);
    List<BatchInterface> mapped = new java.util.ArrayList<>();
    for (GcsUtilLegacy.BatchInterface b : legacy) {
      mapped.add(new BatchAdapter(b));
    }
    return mapped;
  }

  public void remove(Collection<String> filenames) throws IOException {
    delegate.remove(filenames);
  }

  public static class StorageObjectOrIOException {
    final GcsUtilLegacy.StorageObjectOrIOException delegate;

    private StorageObjectOrIOException(GcsUtilLegacy.StorageObjectOrIOException delegate) {
      this.delegate = delegate;
    }

    public static StorageObjectOrIOException create(StorageObject storageObject) {
      return new StorageObjectOrIOException(
          GcsUtilLegacy.StorageObjectOrIOException.create(storageObject));
    }

    public static StorageObjectOrIOException create(IOException ioException) {
      return new StorageObjectOrIOException(
          GcsUtilLegacy.StorageObjectOrIOException.create(ioException));
    }

    static StorageObjectOrIOException fromLegacy(GcsUtilLegacy.StorageObjectOrIOException legacy) {
      return new StorageObjectOrIOException(legacy);
    }

    public @Nullable StorageObject storageObject() {
      return delegate.storageObject();
    }

    public @Nullable IOException ioException() {
      return delegate.ioException();
    }
  }

  public interface BatchInterface {
    <T> void queue(AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb)
        throws IOException;

    void execute() throws IOException;

    int size();
  }

  static class BatchAdapter implements BatchInterface {
    private final GcsUtilLegacy.BatchInterface legacy;

    BatchAdapter(GcsUtilLegacy.BatchInterface legacy) {
      this.legacy = legacy;
    }

    @Override
    public <T> void queue(AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb)
        throws IOException {
      legacy.queue(request, cb);
    }

    @Override
    public void execute() throws IOException {
      legacy.execute();
    }

    @Override
    public int size() {
      return legacy.size();
    }
  }

  static class LegacyBatchAdapter implements GcsUtilLegacy.BatchInterface {
    private final BatchInterface modern;

    LegacyBatchAdapter(BatchInterface modern) {
      this.modern = modern;
    }

    @Override
    public <T> void queue(AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb)
        throws IOException {
      modern.queue(request, cb);
    }

    @Override
    public void execute() throws IOException {
      modern.execute();
    }

    @Override
    public int size() {
      return modern.size();
    }
  }

  public static class RewriteOp {
    final GcsUtilLegacy.RewriteOp delegate;
    Storage.Objects.Rewrite rewriteRequest;

    RewriteOp(GcsUtilLegacy.RewriteOp delegate) {
      this.delegate = delegate;
      this.rewriteRequest = delegate.rewriteRequest;
    }

    public boolean getReadyToEnqueue() {
      return delegate.getReadyToEnqueue();
    }

    public @Nullable GoogleJsonError getLastError() {
      return delegate.getLastError();
    }

    public GcsPath getFrom() {
      return delegate.getFrom();
    }

    public GcsPath getTo() {
      return delegate.getTo();
    }

    public boolean isMetadataOperation() {
      return delegate.isMetadataOperation();
    }

    public void enqueue(BatchInterface batch) throws IOException {
      delegate.enqueue(new LegacyBatchAdapter(batch));
    }
  }
}
