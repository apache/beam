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
package org.apache.beam.sdk.io;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.HashSet;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileSystem.LineageLevel;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class ReadAllViaFileBasedSourceTransform<InT, T>
    extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<T>> {
  public static final boolean DEFAULT_USES_RESHUFFLE = true;
  protected final long desiredBundleSizeBytes;
  protected final SerializableFunction<String, ? extends FileBasedSource<InT>> createSource;
  protected final Coder<T> coder;
  protected final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler;
  protected final boolean usesReshuffle;

  public ReadAllViaFileBasedSourceTransform(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<InT>> createSource,
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
      SerializableFunction<String, ? extends FileBasedSource<InT>> createSource,
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
    return ranges.apply("Read ranges", ParDo.of(readRangesFn())).setCoder(coder);
  }

  protected abstract DoFn<KV<FileIO.ReadableFile, OffsetRange>, T> readRangesFn();

  public static class SplitIntoRangesFn
      extends DoFn<FileIO.ReadableFile, KV<FileIO.ReadableFile, OffsetRange>> {
    private final long desiredBundleSizeBytes;

    // track unique resourceId met. Access it only inside reportSourceLineage
    private transient @Nullable HashSet<ResourceId> uniqueIds;

    public SplitIntoRangesFn(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      MatchResult.Metadata metadata = c.element().getMetadata();
      reportSourceLineage(metadata.resourceId());
      if (!metadata.isReadSeekEfficient()) {
        c.output(KV.of(c.element(), new OffsetRange(0, metadata.sizeBytes())));
        return;
      }
      for (OffsetRange range :
          new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSizeBytes, 0)) {
        c.output(KV.of(c.element(), range));
      }
    }

    /**
     * Report source Lineage. Due to the size limit of Beam metrics, report full file name or only
     * top level depend on the number of files.
     *
     * <p>- Number of files<=100, report full file paths;
     *
     * <p>- Otherwise, report top level only.
     */
    @SuppressWarnings("nullness") // only called in processElement, guaranteed to be non-null
    private void reportSourceLineage(ResourceId resourceId) {
      if (uniqueIds == null) {
        uniqueIds = new HashSet<>();
      } else if (uniqueIds.isEmpty()) {
        // already at capacity
        FileSystems.reportSourceLineage(resourceId, LineageLevel.TOP_LEVEL);
        return;
      }
      uniqueIds.add(resourceId);
      FileSystems.reportSourceLineage(resourceId, LineageLevel.FILE);
      if (uniqueIds.size() >= 100) {
        // avoid reference leak
        uniqueIds.clear();
      }
    }
  }

  public abstract static class AbstractReadFileRangesFn<InT, T>
      extends DoFn<KV<FileIO.ReadableFile, OffsetRange>, T> {
    private final SerializableFunction<String, ? extends FileBasedSource<InT>> createSource;
    private final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler;

    public AbstractReadFileRangesFn(
        SerializableFunction<String, ? extends FileBasedSource<InT>> createSource,
        ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler exceptionHandler) {
      this.createSource = createSource;
      this.exceptionHandler = exceptionHandler;
    }

    protected abstract T makeOutput(
        FileIO.ReadableFile file,
        OffsetRange range,
        FileBasedSource<InT> fileBasedSource,
        BoundedSource.BoundedReader<InT> reader);

    @ProcessElement
    @SuppressFBWarnings(
        value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
        justification = "https://github.com/spotbugs/spotbugs/issues/756")
    public void process(ProcessContext c) throws IOException {
      FileIO.ReadableFile file = c.element().getKey();
      OffsetRange range = c.element().getValue();
      ResourceId resourceId = file.getMetadata().resourceId();
      FileBasedSource<InT> source =
          CompressedSource.from(createSource.apply(resourceId.toString()))
              .withCompression(file.getCompression());
      try (BoundedSource.BoundedReader<InT> reader =
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
