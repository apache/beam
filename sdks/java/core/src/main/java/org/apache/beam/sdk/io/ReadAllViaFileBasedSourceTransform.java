package org.apache.beam.sdk.io;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

@Experimental(Experimental.Kind.SOURCE_SINK)
public abstract class ReadAllViaFileBasedSourceTransform<I, T>
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {
  public static final boolean DEFAULT_USES_RESHUFFLE = true;
  protected final long desiredBundleSizeBytes;
  protected final SerializableFunction<String, ? extends FileBasedSource<I>> createSource;
  protected final Coder<T> coder;
  protected final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler;
  protected final boolean usesReshuffle;

  public ReadAllViaFileBasedSourceTransform(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<I>> createSource,
      Coder<T> coder) {
    this(
        desiredBundleSizeBytes,
        createSource,
        coder,
        DEFAULT_USES_RESHUFFLE,
        new ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler());
  }

  public ReadAllViaFileBasedSourceTransform(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<I>> createSource,
      Coder<T> coder,
      boolean usesReshuffle,
      ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler) {
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.createSource = createSource;
    this.coder = coder;
    this.usesReshuffle = usesReshuffle;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public PCollection<T> expand(PCollection<FileIO.ReadableFile> input) {
    PCollection<KV<FileIO.ReadableFile, OffsetRange>> ranges =
        input.apply("Split into ranges", ParDo.of(new SplitIntoRangesFn(desiredBundleSizeBytes)));
    if (usesReshuffle) {
      ranges = ranges.apply("Reshuffle", Reshuffle.viaRandomKey());
    }
    return ranges
        .apply("Read ranges", ParDo.of(readRangesFn()))
        .setCoder(coder);
  }

  protected abstract DoFn<KV<FileIO.ReadableFile, OffsetRange>, T> readRangesFn();

  public static class SplitIntoRangesFn extends
                                         DoFn<FileIO.ReadableFile, KV<FileIO.ReadableFile, OffsetRange>> {
    private final long desiredBundleSizeBytes;

    public SplitIntoRangesFn(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      MatchResult.Metadata metadata = c.element().getMetadata();
      if (!metadata.isReadSeekEfficient()) {
        c.output(KV.of(c.element(), new OffsetRange(0, metadata.sizeBytes())));
        return;
      }
      for (OffsetRange range :
          new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSizeBytes, 0)) {
        c.output(KV.of(c.element(), range));
      }
    }
  }

  public static abstract class AbstractReadFileRangesFn<I, T> extends DoFn<KV<FileIO.ReadableFile,
      OffsetRange>, T> {
    private final SerializableFunction<String, ? extends FileBasedSource<I>> createSource;
    private final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler;

    public AbstractReadFileRangesFn(
        SerializableFunction<String, ? extends FileBasedSource<I>> createSource,
        ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler) {
      this.createSource = createSource;
      this.exceptionHandler = exceptionHandler;
    }

    protected abstract T makeOutput(FileIO.ReadableFile file, OffsetRange range,
                       FileBasedSource<I> fileBasedSource, BoundedSource.BoundedReader<I> reader);

    @ProcessElement
    @SuppressFBWarnings(
        value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
        justification = "https://github.com/spotbugs/spotbugs/issues/756")
    public void process(ProcessContext c) throws IOException {
      FileIO.ReadableFile file = c.element().getKey();
      OffsetRange range = c.element().getValue();
      FileBasedSource<I> source =
          CompressedSource.from(createSource.apply(file.getMetadata().resourceId().toString()))
              .withCompression(file.getCompression());
      try (BoundedSource.BoundedReader<I> reader =
               source
                   .createForSubrangeOfFile(file.getMetadata(), range.getFrom(), range.getTo())
                   .createReader(c.getPipelineOptions())) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(makeOutput(file, range, source, reader));
        }
      } catch (RuntimeException e) {
        if (exceptionHandler.apply(file, range, e)) {
          throw e;
        }
      }
    }
  }
}
