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

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.smb.BucketShardId.BucketShardIdCoder;
import org.apache.beam.sdk.extensions.smb.FileOperations.Writer;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

/**
 * {@code SortedBucketSink<K, V>} takes a {@code PCollection<V>}, groups it by key into buckets,
 * sort values by key within a bucket, and writes them to files. Each bucket can be further sharded
 * to reduce the impact of hot keys.
 *
 * @param <K> the type of the keys that values in a bucket are sorted with
 * @param <V> the type of the values in a bucket
 */
public class SortedBucketSink<K, V> extends PTransform<PCollection<V>, WriteResult> {

  private final BucketMetadata<K, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final SerializableSupplier<Writer<V>> writerSupplier;
  private final ResourceId tempDirectory;

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      SMBFilenamePolicy filenamePolicy,
      SerializableSupplier<Writer<V>> writerSupplier,
      ResourceId tempDirectory) {
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy = filenamePolicy;
    this.writerSupplier = writerSupplier;
    this.tempDirectory = tempDirectory;
  }

  @Override
  public final WriteResult expand(PCollection<V> input) {
    // @Todo: should we allow windowed writes?
    Preconditions.checkArgument(
        input.isBounded() == IsBounded.BOUNDED,
        "SortedBucketSink cannot be applied to a non-bounded PCollection");

    return input
        .apply("ExtractKeys", ParDo.of(new ExtractKeys<>(this.bucketMetadata)))
        .setCoder(
            KvCoder.of(BucketShardIdCoder.of(), KvCoder.of(ByteArrayCoder.of(), input.getCoder())))
        .apply("GroupByKey", GroupByKey.create())
        .apply("SortValues", SortValues.create(BufferedExternalSorter.options()))
        .apply(
            "WriteOperation",
            new WriteOperation<>(filenamePolicy, bucketMetadata, writerSupplier, tempDirectory));
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  static class ExtractKeys<K, V> extends DoFn<V, KV<BucketShardId, KV<byte[], V>>> {
    private final BucketMetadata<K, V> bucketMetadata;
    private transient int shardId;

    ExtractKeys(BucketMetadata<K, V> bucketMetadata) {
      this.bucketMetadata = bucketMetadata;
    }

    // From Combine.PerKeyWithHotKeyFanout.
    @StartBundle
    public void startBundle() {
      // Spreading a hot key across all possible sub-keys for all bundles
      // would defeat the goal of not overwhelming downstream reducers
      // (as well as making less efficient use of PGBK combining tables).
      // Instead, each bundle independently makes a consistent choice about
      // which "shard" of a key to send its intermediate results.
      shardId =
          ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) % bucketMetadata.getNumShards();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      final V record = c.element();
      final byte[] keyBytes = bucketMetadata.getKeyBytes(record);

      final int bucketId = bucketMetadata.getBucketId(keyBytes);
      c.output(KV.of(BucketShardId.of(bucketId, shardId), KV.of(keyBytes, record)));
    }
  }

  /** The result of a {@link SortedBucketSink} transform. */
  public static class WriteResult implements POutput {
    private final Pipeline pipeline;
    private final PCollection<ResourceId> writtenMetadata;
    private final PCollection<KV<BucketShardId, ResourceId>> writtenFiles;

    WriteResult(
        Pipeline pipeline,
        PCollection<ResourceId> writtenMetadata,
        PCollection<KV<BucketShardId, ResourceId>> writtenFiles) {
      this.pipeline = pipeline;
      this.writtenMetadata = writtenMetadata;
      this.writtenFiles = writtenFiles;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          new TupleTag<>("WrittenMetadata"), writtenMetadata,
          new TupleTag<>("WrittenFiles"), writtenFiles);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  /**
   * Handles writing bucket data and SMB metadata to a uniquely named temp directory. Abstract
   * operation that manages the process of writing to {@link SortedBucketSink}.
   */
  // TODO: Retry policy, etc...
  static class WriteOperation<V>
      extends PTransform<PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>>, WriteResult> {
    private final SMBFilenamePolicy filenamePolicy;
    private final BucketMetadata<?, V> bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;
    private final ResourceId tempDirectory;

    WriteOperation(
        SMBFilenamePolicy filenamePolicy,
        BucketMetadata<?, V> bucketMetadata,
        Supplier<Writer<V>> writerSupplier,
        ResourceId tempDirectory) {
      this.filenamePolicy = filenamePolicy;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
      this.tempDirectory = tempDirectory;
    }

    @Override
    public WriteResult expand(PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>> input) {
      return input
          .apply(
              "WriteTempFiles",
              new WriteTempFiles<>(
                  filenamePolicy.forTempFiles(tempDirectory), bucketMetadata, writerSupplier))
          .apply(
              "FinalizeTempFiles",
              new FinalizeTempFiles<>(
                  filenamePolicy.forDestination(), bucketMetadata, writerSupplier));
    }
  }

  /** Writes metadata and bucket files to temporary location. */
  static class WriteTempFiles<V>
      extends PTransform<
          PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>>, PCollectionTuple> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;

    WriteTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        Supplier<Writer<V>> writerSupplier) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>> input) {

      return PCollectionTuple.of(
              new TupleTag<>("TempMetadata"),
              input.getPipeline().apply(Create.of(Collections.singletonList(writeMetadataFile()))))
          .and(
              new TupleTag<>("TempBuckets"),
              input.apply(
                  ParDo.of(
                      new DoFn<
                          KV<BucketShardId, Iterable<KV<byte[], V>>>,
                          KV<BucketShardId, ResourceId>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) throws Exception {
                          final BucketShardId bucketShardId = c.element().getKey();
                          final Iterable<KV<byte[], V>> records = c.element().getValue();
                          final ResourceId tmpFile =
                              fileAssignment.forBucket(bucketShardId, bucketMetadata);

                          try (final FileOperations.Writer<V> writer = writerSupplier.get()) {
                            writer.prepareWrite(FileSystems.create(tmpFile, writer.getMimeType()));
                            records.forEach(
                                kv -> {
                                  try {
                                    writer.write(kv.getValue());
                                  } catch (Exception e) {
                                    throw new RuntimeException(
                                        String.format(
                                            "Failed to write element %s: %s", kv.getValue(), e));
                                  }
                                });
                          }

                          c.output(KV.of(bucketShardId, tmpFile));
                        }
                      })));
    }

    @SuppressWarnings("unchecked")
    private ResourceId writeMetadataFile() {
      final ResourceId tmpFile = fileAssignment.forMetadata();

      try (final OutputStream outputStream =
          Channels.newOutputStream(FileSystems.create(tmpFile, "application/json"))) {
        BucketMetadata.to(bucketMetadata, outputStream);
      } catch (Exception e) {
        throw new RuntimeException("Metadata write failed: {}", e);
      }
      return tmpFile;
    }
  }

  /** Moves temporary files to final destinations. */
  static class FinalizeTempFiles<V> extends PTransform<PCollectionTuple, WriteResult> {
    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;

    FinalizeTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        Supplier<Writer<V>> writerSupplier) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
    }

    @Override
    public WriteResult expand(PCollectionTuple input) {
      return input.apply(
          "MoveToFinalDestinations",
          PTransform.compose(
              (tuple) -> {
                final PCollection<ResourceId> metadata =
                    tuple
                        .get(new TupleTag<ResourceId>("TempMetadata"))
                        .apply(
                            "RenameMetadata",
                            ParDo.of(
                                new DoFn<ResourceId, ResourceId>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                    final ResourceId dstFile = fileAssignment.forMetadata();
                                    FileSystems.rename(
                                        ImmutableList.of(c.element()), ImmutableList.of(dstFile));
                                    c.output(dstFile);
                                  }
                                }));

                final PCollection<KV<BucketShardId, ResourceId>> buckets =
                    tuple
                        .get(new TupleTag<KV<BucketShardId, ResourceId>>("TempBuckets"))
                        .apply("GroupAll", Group.globally())
                        .apply(
                            "RenameBuckets",
                            ParDo.of(
                                new RenameBuckets<>(
                                    fileAssignment, bucketMetadata, writerSupplier)));

                // @Todo - reduce this to a single FileSystems.rename operation for atomicity?

                return new WriteResult(input.getPipeline(), metadata, buckets);
              }));
    }

    /** Renames temp bucket files to final destinations. */
    static class RenameBuckets<V>
        extends DoFn<Iterable<KV<BucketShardId, ResourceId>>, KV<BucketShardId, ResourceId>> {

      private final FileAssignment fileAssignment;
      private final BucketMetadata bucketMetadata;
      private final Supplier<Writer<V>> writerSupplier;

      RenameBuckets(
          FileAssignment fileAssignment,
          BucketMetadata bucketMetadata,
          Supplier<Writer<V>> writerSupplier) {
        this.fileAssignment = fileAssignment;
        this.bucketMetadata = bucketMetadata;
        this.writerSupplier = writerSupplier;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        // Create missing files
        Set<BucketShardId> missingFiles = new HashSet<>();
        for (int i = 0; i < bucketMetadata.getNumBuckets(); i++) {
          for (int j = 0; j < bucketMetadata.getNumShards(); j++) {
            missingFiles.add(BucketShardId.of(i, j));
          }
        }
        final Iterable<KV<BucketShardId, ResourceId>> writtenFiles = c.element();
        writtenFiles.forEach(k -> missingFiles.remove(k.getKey()));

        for (BucketShardId id : missingFiles) {
          final ResourceId dstFile = fileAssignment.forBucket(id, bucketMetadata);

          try (final Writer<?> writer = writerSupplier.get()) {
            writer.prepareWrite(FileSystems.create(dstFile, writer.getMimeType()));
          }
          c.output(KV.of(id, dstFile));
        }

        // Rename written files
        final List<ResourceId> srcFiles = new ArrayList<>();
        final List<ResourceId> dstFiles = new ArrayList<>();
        final List<KV<BucketShardId, ResourceId>> finalBucketLocations = new ArrayList<>();

        writtenFiles.forEach(
            kv -> {
              ResourceId tmpFile = kv.getValue();
              BucketShardId id = kv.getKey();
              final ResourceId dstFile = fileAssignment.forBucket(id, bucketMetadata);

              srcFiles.add(tmpFile);
              dstFiles.add(dstFile);
              finalBucketLocations.add(KV.of(id, dstFile));
            });

        FileSystems.rename(srcFiles, dstFiles);
        finalBucketLocations.forEach(c::output);
      }
    }
  }
}
