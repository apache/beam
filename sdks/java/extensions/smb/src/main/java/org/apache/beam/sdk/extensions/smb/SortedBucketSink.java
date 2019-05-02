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

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes a PCollection representing sorted, bucketized data to files, where the # of files is equal
 * to the # of buckets, assuming the contents of each bucket fit on a single worker.
 *
 * @param <KeyT>
 * @param <ValueT>
 */
public class SortedBucketSink<KeyT, ValueT> extends PTransform<PCollection<ValueT>, WriteResult> {

  private final BucketMetadata<KeyT, ValueT> bucketingMetadata;
  private final SMBFilenamePolicy smbFilenamePolicy;
  private final Supplier<Writer<ValueT>> writerSupplier;
  private final ResourceId tempDirectory;

  public SortedBucketSink(
      BucketMetadata<KeyT, ValueT> bucketingMetadata,
      SMBFilenamePolicy smbFilenamePolicy,
      Supplier<Writer<ValueT>> writerSupplier,
      ResourceId tempDirectory) {
    this.bucketingMetadata = bucketingMetadata;
    this.smbFilenamePolicy = smbFilenamePolicy;
    this.writerSupplier = writerSupplier;
    this.tempDirectory = tempDirectory;
  }

  @Override
  public final WriteResult expand(PCollection<ValueT> input) {
    final Coder<KeyT> sortingKeyCoder;
    try {
      sortingKeyCoder = bucketingMetadata.getSortingKeyCoder();
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Could not find a coder for key type", e);
    }

    return input
        .apply("Assign buckets", ParDo.of(new ExtractBucketAndSortKey<>(this.bucketingMetadata)))
        .setCoder(KvCoder.of(VarIntCoder.of(), KvCoder.of(sortingKeyCoder, input.getCoder())))
        .apply("Group per bucket", GroupByKey.create())
        .apply("Sort values in bucket", SortValues.create(BufferedExternalSorter.options()))
        .apply(
            "Write bucket data",
            new WriteOperation<>(
                smbFilenamePolicy, bucketingMetadata, writerSupplier, tempDirectory));
  }

  /*
   *
   */
  static final class ExtractBucketAndSortKey<K, V> extends DoFn<V, KV<Integer, KV<K, V>>> {
    private final BucketMetadata<K, V> bucketMetadata;

    ExtractBucketAndSortKey(BucketMetadata<K, V> bucketMetadata) {
      this.bucketMetadata = bucketMetadata;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      final V record = c.element();
      final K key = bucketMetadata.extractSortingKey(record);
      final byte[] keyBytes = bucketMetadata.keyToBytes(key);

      final int bucket =
          Math.abs(bucketMetadata.getHashFunction().hashBytes(keyBytes).asInt())
              % bucketMetadata.getNumBuckets();

      c.output(KV.of(bucket, KV.of(key, record)));
    }
  }

  /**
   * Represents a successful write to temp directory that was moved to its final output destination.
   */
  public static final class WriteResult implements POutput {
    private final Pipeline pipeline;
    private final PCollection<ResourceId> writtenMetadata;
    private final PCollection<KV<Integer, ResourceId>> writtenBuckets;

    WriteResult(
        Pipeline pipeline,
        PCollection<ResourceId> writtenMetadata,
        PCollection<KV<Integer, ResourceId>> writtenBuckets) {
      this.pipeline = pipeline;
      this.writtenMetadata = writtenMetadata;
      this.writtenBuckets = writtenBuckets;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(
          new TupleTag<>("SortedBucketsWritten"), writtenBuckets,
          new TupleTag<>("SMBMetadataWritten"), writtenMetadata);
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }

  /**
   * Handles writing bucket data and SMB metadata to a uniquely named temp directory. @Todo: Retry
   * policy, sharding per bucket, etc...
   */
  static final class WriteOperation<K, V>
      extends PTransform<PCollection<KV<Integer, Iterable<KV<K, V>>>>, WriteResult> {
    private final SMBFilenamePolicy smbFilenamePolicy;
    private final BucketMetadata<K, V> bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;
    private final ResourceId tempDirectory;

    WriteOperation(
        SMBFilenamePolicy smbFilenamePolicy,
        BucketMetadata<K, V> bucketMetadata,
        Supplier<Writer<V>> writerSupplier,
        ResourceId tempDirectory) {
      this.smbFilenamePolicy = smbFilenamePolicy;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
      this.tempDirectory = tempDirectory;
    }

    @Override
    public WriteResult expand(PCollection<KV<Integer, Iterable<KV<K, V>>>> input) {
      return input
          .apply(
              "Write buckets to temp directory",
              new WriteTempFiles<>(
                  smbFilenamePolicy.forTempFiles(tempDirectory), bucketMetadata, writerSupplier))
          .apply(
              "Finalize temp file destinations",
              new FinalizeTempFiles(smbFilenamePolicy.forDestination(), bucketMetadata));
    }
  }

  static class WriteTempFiles<K, V>
      extends PTransform<PCollection<KV<Integer, Iterable<KV<K, V>>>>, PCollectionTuple> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteTempFiles.class);

    private final FileAssignment tempFileAssignment;
    private final BucketMetadata bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;

    WriteTempFiles(
        FileAssignment tempFileAssignment,
        BucketMetadata bucketMetadata,
        Supplier<Writer<V>> writerSupplier) {
      this.tempFileAssignment = tempFileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<Integer, Iterable<KV<K, V>>>> input) {

      return PCollectionTuple.of(
              new TupleTag<>("tempMetadata"),
              input.getPipeline().apply(Create.of(Collections.singletonList(writeMetadataFile()))))
          .and(
              new TupleTag<>("tempBuckets"),
              input.apply(
                  ParDo.of(
                      new DoFn<KV<Integer, Iterable<KV<K, V>>>, KV<Integer, ResourceId>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) throws Exception {
                          final Integer bucketId = c.element().getKey();
                          final Iterable<KV<K, V>> records = c.element().getValue();

                          final ResourceId tmpDst =
                              tempFileAssignment.forBucket(
                                  bucketId, bucketMetadata.getNumBuckets());

                          final FileOperations.Writer<V> writer = writerSupplier.get();

                          writer.prepareWrite(FileSystems.create(tmpDst, writer.getMimeType()));

                          try {
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
                          } finally {
                            writer.finishWrite();
                          }
                          c.output(KV.of(bucketId, tmpDst));
                        }
                      })));
    }

    private ResourceId writeMetadataFile() {
      final ResourceId file = tempFileAssignment.forMetadata();
      WritableByteChannel channel = null;
      try {
        channel = FileSystems.create(file, "application/json");
        BucketMetadata.to(bucketMetadata, Channels.newOutputStream(channel));
        // new ObjectMapper().writeValue(Channels.newOutputStream(channel), bucketMetadata);
      } catch (Exception e) {
        closeChannelOrThrow(channel, e);
      }
      return file;
    }

    private static void closeChannelOrThrow(WritableByteChannel channel, Exception prior) {
      try {
        channel.close();
      } catch (Exception e) {
        LOG.error("Closing channel failed: {}", e);
        throw new RuntimeException(prior);
      }
    }
  }

  /** Moves written temp files to their final destinations. Input is a map of bucket -> temp path */
  static final class FinalizeTempFiles extends PTransform<PCollectionTuple, WriteResult> {
    private final FileAssignment finalizedFileAssignment;
    private final BucketMetadata bucketMetadata;

    FinalizeTempFiles(FileAssignment fileAssignment, BucketMetadata bucketMetadata) {
      this.finalizedFileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
    }

    @Override
    public WriteResult expand(PCollectionTuple input) {
      return input.apply(
          "Move to final destinations",
          PTransform.compose(
              (tuple) -> {
                final PCollection<ResourceId> metadata =
                    tuple
                        .get(new TupleTag<ResourceId>("tempMetadata"))
                        .apply(
                            ParDo.of(
                                new DoFn<ResourceId, ResourceId>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                    final ResourceId finalMetadataDst =
                                        finalizedFileAssignment.forMetadata();
                                    FileSystems.rename(
                                        ImmutableList.of(c.element()),
                                        ImmutableList.of(finalMetadataDst));
                                    c.output(finalMetadataDst);
                                  }
                                }));

                final PCollection<KV<Integer, ResourceId>> buckets =
                    tuple
                        .get(new TupleTag<KV<Integer, ResourceId>>("tempBuckets"))
                        .apply("Collect all written buckets", Group.globally())
                        .apply(
                            "Rename temp buckets",
                            ParDo.of(
                                new DoFn<
                                    Iterable<KV<Integer, ResourceId>>, KV<Integer, ResourceId>>() {
                                  @ProcessElement
                                  public void processElement(ProcessContext c) throws Exception {
                                    final List<ResourceId> srcFiles = new ArrayList<>();
                                    final List<ResourceId> dstFiles = new ArrayList<>();

                                    final List<KV<Integer, ResourceId>> finalBucketLocations =
                                        new ArrayList<>();

                                    c.element()
                                        .forEach(
                                            bucketAndTempLocation -> {
                                              srcFiles.add(bucketAndTempLocation.getValue());

                                              final ResourceId dstFile =
                                                  finalizedFileAssignment.forBucket(
                                                      bucketAndTempLocation.getKey(),
                                                      bucketMetadata.getNumBuckets());

                                              dstFiles.add(dstFile);
                                              finalBucketLocations.add(
                                                  KV.of(bucketAndTempLocation.getKey(), dstFile));
                                            });

                                    FileSystems.rename(srcFiles, dstFiles);

                                    finalBucketLocations.forEach(c::output);
                                  }
                                }));

                // @Todo - reduce this to a single FileSystems.rename operation for atomicity?

                return new WriteResult(input.getPipeline(), metadata, buckets);
              }));
    }
  }
}
