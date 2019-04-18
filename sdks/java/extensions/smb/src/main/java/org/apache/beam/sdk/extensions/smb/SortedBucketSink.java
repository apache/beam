package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

/**
 * Writes a PCollection representing sorted, bucketized data to files, where the # of files is
 * equal to the # of buckets, assuming the contents of each bucket fit on a single worker.
 *
 * This must be implemented for different file-based IO types i.e. Avro.
 */
public abstract class SortedBucketSink<SortingKeyT, ValueT> extends
    PTransform<PCollection<ValueT>, WriteResult> {

  private final BucketMetadata<SortingKeyT, ValueT> bucketingMetadata;
  private final FilenamePolicy filenamePolicy; // @Todo: custom SMB FilenamePolicy impl?
  private final OutputFileHints outputFileHints;
  private final ValueProvider<ResourceId> tempDirectoryProvider;

  public SortedBucketSink(
      BucketMetadata<SortingKeyT, ValueT> bucketingMetadata,
      FilenamePolicy filenamePolicy,
      OutputFileHints outputFileHints,
      ValueProvider<ResourceId> tempDirectoryProvider) {
    this.bucketingMetadata = bucketingMetadata;
    this.tempDirectoryProvider = tempDirectoryProvider;
    this.filenamePolicy = filenamePolicy;
    this.outputFileHints = outputFileHints;
  }

  abstract Writer<ValueT> createWriter();

  /**
   * Must be implemented per Sink. Handles the actual writes of the underlying value type V.
   */
  abstract static class Writer<V> implements Serializable {
    abstract void open(WritableByteChannel channel) throws Exception;
    abstract void append(V value) throws Exception;
    abstract void close() throws Exception;
  }

  // @Todo: This assumes all buckets will fit in memory and write to a single file...
  // @Todo: improved sorting
  @Override
  public WriteResult expand(PCollection<ValueT> input) {
    final Coder<KV<Integer, KV<SortingKeyT, ValueT>>> bucketedCoder = KvCoder.of(
        VarIntCoder.of(),
        KvCoder.of(this.bucketingMetadata.getSortingKeyCoder(), input.getCoder())
    );  // @Todo Verify deterministic? Maybe metadata class can validate SortingKeyCoder

    return input
        .apply("Assign buckets", ParDo.of(new DoFn<ValueT, KV<Integer, KV<SortingKeyT, ValueT>>>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            ValueT record = c.element();

            c.output(KV.of(
                bucketingMetadata.assignBucket(record),
                KV.of(bucketingMetadata.extractSortingKey(record), record)
            ));
          }})).setCoder(bucketedCoder)
        .apply("Group per bucket", GroupByKey.create()) // @Todo: Verify fusion of these steps
        .apply("Sort values in bucket", SortValues.create(BufferedExternalSorter.options()))
        .apply("Write bucket data", new WriteOperation());
  }

  /**
   * Represents a successful write to temp directory that was moved to its final output destination.
   */
  static class WriteResult implements POutput, Serializable {
    private final Pipeline pipeline;
    private final PCollection<KV<Integer, ResourceId>> writtenBuckets;

    WriteResult(
        Pipeline pipeline,
        PCollection<KV<Integer, ResourceId>> writtenBuckets) {
      this.pipeline = pipeline;
      this.writtenBuckets = writtenBuckets;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.of(new TupleTag<>("SMBWriteResult"), writtenBuckets);
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input,
        PTransform<?, ?> transform) { }
  }

  /**
   * Handles writing bucket data and SMB metadata to a uniquely named temp directory.
   * @Todo: Retry policy, sharding per bucket, etc...
   */
  public class WriteOperation extends
      PTransform<PCollection<KV<Integer, Iterable<KV<SortingKeyT, ValueT>>>>, WriteResult> {

    // Based off of FileBasedSink's temp directory resolution; not completely in parity yet
    private final String TEMPDIR_TIMESTAMP = "yyyy-MM-dd_HH-mm-ss";
    private final String BEAM_TEMPDIR_PATTERN = ".temp-beam-%s";
    private final ResourceId tempDirectory;

    WriteOperation() {
      tempDirectory = tempDirectoryProvider.get()
          .resolve(String.format(BEAM_TEMPDIR_PATTERN, Instant.now().toString(DateTimeFormat.forPattern(TEMPDIR_TIMESTAMP))),
              StandardResolveOptions.RESOLVE_DIRECTORY);
    }

    @Override
    public WriteResult expand(PCollection<KV<Integer, Iterable<KV<SortingKeyT, ValueT>>>> input) {
      // Try writing metadata first.
      // @Todo verify the call site where this is actually getting executed. Maybe it should be
      //       its own ptransform
      final ResourceId metadataResource;

      try {
        metadataResource = writeSMBMetadata();
      } catch (IOException e) {
        throw new RuntimeException("Failed to write SMB metadata to temp directory: {}", e);
      }

      return input
          .apply("Write buckets to temp directory", ParDo.of(new WriteTempFiles()))
          .apply("Finalize temp file destinations", new FinalizeTempFiles());
    }

    private ResourceId writeSMBMetadata() throws IOException {
      // @Todo
      return null;
    }
  }

  class WriteTempFiles extends DoFn<KV<Integer, Iterable<KV<SortingKeyT, ValueT>>>, KV<Integer, ResourceId>> {
    WriteTempFiles() {
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      final Iterable<KV<SortingKeyT, ValueT>> values = c.element().getValue();
      final Integer bucket = c.element().getKey();

      // @Todo write temp files per bucket
    }
  }

  /**
   * Moves written temp files to their final destinations. Input is a map of bucket -> temp path
   */
  class FinalizeTempFiles extends PTransform<PCollection<KV<Integer, ResourceId>>, WriteResult> {
    FinalizeTempFiles() {
    }

    @Override
    public WriteResult expand(PCollection<KV<Integer, ResourceId>> input) {
      return input
          .apply("Move to final destinations", PTransform.compose((writtenTempFiles) -> {
            // @Todo move temp buckets + metadata to final destination

            return new WriteResult(input.getPipeline(), writtenTempFiles);
          }));
    }
  }
}
