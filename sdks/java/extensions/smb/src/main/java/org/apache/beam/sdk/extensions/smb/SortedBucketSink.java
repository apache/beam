package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
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
 * Writes a PCollection representing sorted, bucketized data to files, where the # of files is
 * equal to the # of buckets, assuming the contents of each bucket fit on a single worker.
 *
 * This must be implemented for different file-based IO types i.e. Avro.
 */
public abstract class SortedBucketSink<SortingKeyT, ValueT> extends
    PTransform<PCollection<ValueT>, WriteResult> {

  private final BucketMetadata<SortingKeyT, ValueT> bucketingMetadata;
  private final SMBFilenamePolicy smbFilenamePolicy;
  private final Supplier<Writer<ValueT>> writerSupplier;

  public SortedBucketSink(
      BucketMetadata<SortingKeyT, ValueT> bucketingMetadata,
      SMBFilenamePolicy smbFilenamePolicy,
      Supplier<Writer<ValueT>> writerSupplier
  ) {
    this.bucketingMetadata = bucketingMetadata;
    this.smbFilenamePolicy = smbFilenamePolicy;
    this.writerSupplier = writerSupplier;
  }

  @Override
  final public WriteResult expand(PCollection<ValueT> input) {
    final Coder<KV<Integer, KV<SortingKeyT, ValueT>>> bucketedCoder = KvCoder.of(
        VarIntCoder.of(),
        KvCoder.of(this.bucketingMetadata.getSortingKeyCoder(), input.getCoder())
    );

    return input
        .apply("Assign buckets", ParDo.of(
            new ExtractBucketAndSortKey<SortingKeyT, ValueT>(this.bucketingMetadata))
        ).setCoder(bucketedCoder) // @Todo: Verify fusion of these steps
        .apply("Group per bucket", GroupByKey.create())
        .apply("Sort values in bucket", SortValues.create(BufferedExternalSorter.options()))
        .apply("Write bucket data", new WriteOperation<>(
            smbFilenamePolicy, bucketingMetadata, writerSupplier));
  }

  /*
   *
   */
  static final class ExtractBucketAndSortKey<S, V> extends DoFn<V, KV<Integer, KV<S, V>>> {
    private final BucketMetadata<S, V> bucketMetadata;

    ExtractBucketAndSortKey(BucketMetadata<S, V> bucketMetadata) {
      this.bucketMetadata = bucketMetadata;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      V record = c.element();
      c.output(KV.of(
          bucketMetadata.assignBucket(record),
          KV.of(bucketMetadata.extractSortingKey(record), record)
      ));
    }
  }

  /**
   * Represents a successful write to temp directory that was moved to its final output destination.
   */
  static final class WriteResult implements POutput {
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
    public void finishSpecifyingOutput(String transformName, PInput input,
        PTransform<?, ?> transform) { }
  }

  /**
   * Handles writing bucket data and SMB metadata to a uniquely named temp directory.
   * @Todo: Retry policy, sharding per bucket, etc...
   */
  static final class WriteOperation<S, V> extends
      PTransform<PCollection<KV<Integer, Iterable<KV<S, V>>>>, WriteResult> {
    private final SMBFilenamePolicy smbFilenamePolicy;
    private final BucketMetadata<S, V> bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;

    WriteOperation(
        SMBFilenamePolicy smbFilenamePolicy,
        BucketMetadata<S, V> bucketMetadata,
        Supplier<Writer<V>> writerSupplier) {
      this.smbFilenamePolicy = smbFilenamePolicy;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
    }

    @Override
    public WriteResult expand(PCollection<KV<Integer, Iterable<KV<S, V>>>> input) {
      return input
          .apply(
              "Write buckets to temp directory",
              new WriteTempFiles<>(smbFilenamePolicy.forTempFiles(), bucketMetadata, writerSupplier))
          .apply("Finalize temp file destinations",
              new FinalizeTempFiles(smbFilenamePolicy.forDestination(), bucketMetadata)
          );
    }
  }

  static class WriteTempFiles<S, V> extends PTransform<
      PCollection<KV<Integer, Iterable<KV<S, V>>>>, PCollectionTuple> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteTempFiles.class);

    private final FileAssignment tempFileAssignment;
    private final BucketMetadata bucketMetadata;
    private final Supplier<Writer<V>> writerSupplier;


    WriteTempFiles(
        FileAssignment tempFileAssignment,
        BucketMetadata bucketMetadata,
        Supplier<Writer<V>> writerSupplier
        ) {
      this.tempFileAssignment = tempFileAssignment;
      this.bucketMetadata = bucketMetadata;
      this.writerSupplier = writerSupplier;
    }

    @Override
    public PCollectionTuple expand(
        PCollection<KV<Integer, Iterable<KV<S, V>>>> input) {

     return PCollectionTuple
         .of(
             new TupleTag<ResourceId>("tempMetadata"),
             input.getPipeline().apply(Create.of(Collections.singletonList(writeMetadataFile()))))
         .and(
             new TupleTag<KV<Integer, ResourceId>>("tempBuckets"),
             input.apply(ParDo.of(
                 new DoFn<KV<Integer, Iterable<KV<S, V>>>, KV<Integer, ResourceId>>() {
                   @ProcessElement
                   public void processElement(ProcessContext c) throws Exception {
                     final Integer bucketId = c.element().getKey();
                     final Iterable<KV<S,V>> records = c.element().getValue();

                     final ResourceId tmpDst = tempFileAssignment
                         .forBucketShard(bucketId, bucketMetadata.getNumBuckets(), 1, 1);

                     final SortedBucketFile.Writer<V> writer = writerSupplier.get();

                     writer.prepareWrite(FileSystems.create(tmpDst, writer.getMimeType()));

                     try {
                       records.forEach(kv -> {
                         try {
                           writer.write(kv.getValue());
                         } catch (Exception e) {
                           throw new RuntimeException(
                               String.format("Failed to write element %s: %s", kv.getValue(), e));
                         }
                       });
                     } finally {
                       writer.finishWrite();
                     }
                     c.output(KV.of(bucketId, tmpDst));
                   }
                 })
             )
         );
    }

    private ResourceId writeMetadataFile() {
      final ResourceId file = tempFileAssignment.forMetadata();
      WritableByteChannel channel = null;
      try {
        channel = FileSystems.create(file, "application/json");
        new ObjectMapper().writeValue(Channels.newOutputStream(channel), bucketMetadata);
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

  /**
   * Moves written temp files to their final destinations. Input is a map of bucket -> temp path
   */
  static final class FinalizeTempFiles extends PTransform<PCollectionTuple, WriteResult> {
    private final FileAssignment finalizedFileAssignment;
    private final BucketMetadata bucketMetadata;

    FinalizeTempFiles(
        FileAssignment fileAssignment,
        BucketMetadata bucketMetadata
    ) {
      this.finalizedFileAssignment = fileAssignment;
      this.bucketMetadata = bucketMetadata;
    }

    @Override
    public WriteResult expand(PCollectionTuple input) {
      return input
          .apply("Move to final destinations", PTransform.compose((tuple) -> {
            final PCollection<ResourceId> metadata = tuple
                .get(new TupleTag<ResourceId>("tempMetadata"))
                .apply(ParDo.of(
                    new DoFn<ResourceId, ResourceId>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        final ResourceId finalMetadataDst = finalizedFileAssignment.forMetadata();
                        FileSystems.rename(
                            ImmutableList.of(c.element()),
                            ImmutableList.of(finalMetadataDst));
                        c.output(finalMetadataDst);
                      }
                    }));

            final PCollection<KV<Integer, ResourceId>> buckets = tuple
                .get(new TupleTag<KV<Integer, ResourceId>>("tempBuckets"))
                .apply("Collect all written buckets", Group.globally())
                .apply("Rename temp buckets", ParDo.of(
                    new DoFn<Iterable<KV<Integer, ResourceId>>, KV<Integer, ResourceId>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        final List<ResourceId> srcFiles = new ArrayList<>();
                        final List<ResourceId> dstFiles = new ArrayList<>();

                        final List<KV<Integer, ResourceId>> finalBucketLocations = new ArrayList<>();

                        c.element().forEach(bucketAndTempLocation -> {
                          srcFiles.add(bucketAndTempLocation.getValue());

                          final ResourceId dstFile = finalizedFileAssignment
                              .forBucketShard(bucketAndTempLocation.getKey(), bucketMetadata.getNumBuckets(), 1, 1);

                          dstFiles.add(dstFile);
                          finalBucketLocations.add(KV.of(bucketAndTempLocation.getKey(), dstFile));
                        });

                        FileSystems.rename(srcFiles, dstFiles);

                        finalBucketLocations.forEach(c::output);
                      }
                    }));

            // @Todo Cleanup if either write failed (right now it's not totally atomic...)

            return new WriteResult(input.getPipeline(), metadata, buckets);
          }));
    }
  }
}
