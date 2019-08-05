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
import org.apache.beam.sdk.extensions.sorter.ExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A {@link PTransform} for writing a {@link PCollection} to file-based sink, where files represent
 * "buckets" of elements deterministically assigned by {@link BucketMetadata} based on a key
 * extraction function. The elements in each bucket are written in sorted order according to the
 * same key.
 *
 * <p>This transform is intended to be used in conjunction with the {@link SortedBucketSource}
 * transform. Any two datasets written with {@link SortedBucketSink} using the same bucketing scheme
 * can be joined by simply sequentially reading and merging files, thus eliminating the shuffle
 * required by {@link GroupByKey}-based transforms. This is ideal for datasets that will be written
 * once and read many times with a predictable join key, i.e. user event data.
 *
 * <h3>Transform steps</h3>
 *
 * <p>{@link SortedBucketSink} re-uses existing {@link PTransform}s to map over each element,
 * extract a {@code byte[]} representation of its sorting key using {@link
 * BucketMetadata#getKeyBytes(Object)}, and assign it to an Integer bucket using {@link
 * BucketMetadata#getBucketId(byte[])}. Next, a {@link GroupByKey} transform is applied to create a
 * {@link PCollection} of {@code N} elements, where {@code N} is the number of buckets specified by
 * {@link BucketMetadata#getNumBuckets()}, then a {@link SortValues} transform is used to sort
 * elements within each bucket group. Finally, the write operation is performed, where each bucket
 * is first written to a {@link SortedBucketSink#tempDirectory} and then copied to its final
 * destination.
 *
 * <p>A JSON-serialized form of {@link BucketMetadata} is also written, which is required in order
 * to join {@link SortedBucketSink}s using the {@link SortedBucketSource} transform.
 *
 * <h3>Bucketing properties and hot keys</h3>
 *
 * <p>Bucketing properties are specified in {@link BucketMetadata}. The number of buckets, {@code
 * N}, must be a power of two and should be chosen such that each bucket can fit in a worker node's
 * memory. Note that the {@link SortValues} transform will try to sort in-memory and fall back to an
 * {@link ExternalSorter} if needed.
 *
 * <p>Each bucket can be further sharded to reduce the impact of hot keys, by specifying {@link
 * BucketMetadata#getNumShards()}.
 *
 * @param <K> the type of the keys that values in a bucket are sorted with
 * @param <V> the type of the values in a bucket
 */
public class SortedBucketSink<K, V> extends PTransform<PCollection<V>, WriteResult> {

  private final BucketMetadata<K, V> bucketMetadata;
  private final SMBFilenamePolicy filenamePolicy;
  private final ResourceId tempDirectory;
  private final FileOperations<V> fileOperations;
  private final int sorterMemoryMb;

  public SortedBucketSink(
      BucketMetadata<K, V> bucketMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations,
      int sorterMemoryMb) {
    this.bucketMetadata = bucketMetadata;
    this.filenamePolicy = new SMBFilenamePolicy(outputDirectory, filenameSuffix);
    this.tempDirectory = tempDirectory;
    this.fileOperations = fileOperations;
    this.sorterMemoryMb = sorterMemoryMb;
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
        .apply(
            "SortValues",
            SortValues.create(
                BufferedExternalSorter.options()
                    .withExternalSorterType(ExternalSorter.Options.SorterType.NATIVE)
                    .withMemoryMB(sorterMemoryMb)))
        .apply(
            "WriteOperation",
            new WriteOperation<>(filenamePolicy, bucketMetadata, fileOperations, tempDirectory));
  }

  /** Extract bucket and shard id for grouping, and key bytes for sorting. */
  private static class ExtractKeys<K, V> extends DoFn<V, KV<BucketShardId, KV<byte[], V>>> {
    // Substitute null keys in the output KV<byte[], V> so that they survive serialization
    private static final byte[] NULL_SORT_KEY = new byte[0];

    ExtractKeys(BucketMetadata<K, V> bucketMetadata) {
      this.bucketMetadata = bucketMetadata;
    }

    private final BucketMetadata<K, V> bucketMetadata;
    private transient int shardId;

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

      final BucketShardId bucketShardId =
          keyBytes != null
              ? BucketShardId.of(bucketMetadata.getBucketId(keyBytes), shardId)
              : BucketShardId.ofNullKey(shardId);

      final byte[] sortKey = keyBytes != null ? keyBytes : NULL_SORT_KEY;
      c.output(KV.of(bucketShardId, KV.of(sortKey, record)));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.delegate(bucketMetadata);
    }
  }

  /**
   * The result of a successfully completed {@link SortedBucketSink} transform. Holds {@link
   * TupleTag} references to both the successfully written {@link BucketMetadata}, and to all
   * successfully written {@link BucketShardId}s.
   */
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
  private static class WriteOperation<V>
      extends PTransform<PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>>, WriteResult> {
    private final SMBFilenamePolicy filenamePolicy;
    private final BucketMetadata<?, V> bucketMetadata;
    private final FileOperations<V> fileOperations;
    private final ResourceId tempDirectory;

    WriteOperation(
        SMBFilenamePolicy filenamePolicy,
        BucketMetadata<?, V> bucketMetadata,
        FileOperations<V> fileOperations,
        ResourceId tempDirectory) {
      this.filenamePolicy = filenamePolicy;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
      this.tempDirectory = tempDirectory;
    }

    @Override
    public WriteResult expand(PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>> input) {
      return input
          .apply(
              "WriteTempFiles",
              new WriteTempFiles<>(
                  filenamePolicy.forTempFiles(tempDirectory), bucketMetadata, fileOperations))
          .apply(
              "FinalizeTempFiles",
              new FinalizeTempFiles<>(
                  filenamePolicy.forDestination(), bucketMetadata, fileOperations));
    }
  }

  /** Writes metadata and bucket files to temporary location. */
  private static class WriteTempFiles<V>
      extends PTransform<
          PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>>, PCollectionTuple> {

    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;

    WriteTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        FileOperations<V> fileOperations) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<BucketShardId, Iterable<KV<byte[], V>>>> input) {

      return PCollectionTuple.of(
              new TupleTag<>("TempMetadata"),
              input
                  .getPipeline()
                  .apply(
                      "WriteTempMetadata",
                      Create.of(Collections.singletonList(writeMetadataFile()))))
          .and(
              new TupleTag<>("TempBuckets"),
              input.apply(
                  "WriteTempBuckets",
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

                          try (final FileOperations.Writer<V> writer =
                              fileOperations.createWriter(tmpFile)) {
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

                        @Override
                        public void populateDisplayData(Builder builder) {
                          super.populateDisplayData(builder);
                          builder.delegate(fileAssignment);
                          builder.delegate(fileOperations);
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
  private static class FinalizeTempFiles<V> extends PTransform<PCollectionTuple, WriteResult> {
    private final FileAssignment fileAssignment;
    private final BucketMetadata bucketMetadata;
    private final FileOperations<V> fileOperations;

    FinalizeTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata,
        FileOperations<V> fileOperations) {
      this.fileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.fileOperations = fileOperations;
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

                                  @Override
                                  public void populateDisplayData(Builder builder) {
                                    super.populateDisplayData(builder);
                                    builder.add(
                                        DisplayData.item(
                                            "Metadata Location",
                                            fileAssignment.forMetadata().toString()));
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
                                    fileAssignment, bucketMetadata, fileOperations)));

                // @Todo - reduce this to a single FileSystems.rename operation for atomicity?

                return new WriteResult(input.getPipeline(), metadata, buckets);
              }));
    }

    /** Renames temp bucket files to final destinations. */
    private static class RenameBuckets<V>
        extends DoFn<Iterable<KV<BucketShardId, ResourceId>>, KV<BucketShardId, ResourceId>> {

      private final FileAssignment fileAssignment;
      private final BucketMetadata bucketMetadata;
      private final FileOperations<V> fileOperations;

      RenameBuckets(
          FileAssignment fileAssignment,
          BucketMetadata bucketMetadata,
          FileOperations<V> fileOperations) {
        this.fileAssignment = fileAssignment;
        this.bucketMetadata = bucketMetadata;
        this.fileOperations = fileOperations;
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

          try (final Writer<?> ignored = fileOperations.createWriter(dstFile)) {}
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

      @Override
      public void populateDisplayData(Builder builder) {
        super.populateDisplayData(builder);
        builder.delegate(fileAssignment);
      }
    }
  }
}
