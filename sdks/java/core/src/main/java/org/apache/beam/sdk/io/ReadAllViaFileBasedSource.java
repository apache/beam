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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads each file in the input {@link PCollection} of {@link ReadableFile} using given parameters
 * for splitting files into offset ranges and for creating a {@link FileBasedSource} for a file. The
 * input {@link PCollection} must not contain {@link ResourceId#isDirectory directories}.
 *
 * <p>To obtain the collection of {@link ReadableFile} from a filepattern, use {@link
 * FileIO#readMatches()}.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ReadAllViaFileBasedSource<T>
    extends PTransform<PCollection<ReadableFile>, PCollection<T>> {
  private final long desiredBundleSizeBytes;
  private final SerializableFunction<String, ? extends FileBasedSource<T>> createSource;
  private final Coder<T> coder;

  public ReadAllViaFileBasedSource(
      long desiredBundleSizeBytes,
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      Coder<T> coder) {
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.createSource = createSource;
    this.coder = coder;
  }

  @Override
  public PCollection<T> expand(PCollection<ReadableFile> input) {
    return input
        .apply("Split into ranges", ParDo.of(new SplitIntoRangesFn(desiredBundleSizeBytes)))
        .apply("Reshuffle", Reshuffle.viaRandomKey())
        .apply("Create sources", ParDo.of(new CreateBoundedSourceFn<>(createSource)))
        .setCoder(new BoundedSourceCoder<>())
        .apply("Read ranges", ParDo.of(new ReadFromBoundedSourceFn<>()))
        .setCoder(coder);
  }

  private static class SplitIntoRangesFn extends DoFn<ReadableFile, KV<ReadableFile, OffsetRange>> {
    private final long desiredBundleSizeBytes;

    private SplitIntoRangesFn(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      Metadata metadata = c.element().getMetadata();
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

  private static class CreateBoundedSourceFn<T>
      extends DoFn<KV<ReadableFile, OffsetRange>, BoundedSource<T>> {
    private final SerializableFunction<String, ? extends FileBasedSource<T>> createSource;

    private CreateBoundedSourceFn(
        SerializableFunction<String, ? extends FileBasedSource<T>> createSource) {
      this.createSource = createSource;
    }

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
      ReadableFile file = c.element().getKey();
      OffsetRange range = c.element().getValue();
      c.output(
          CompressedSource.from(createSource.apply(file.getMetadata().resourceId().toString()))
              .withCompression(file.getCompression())
              .createForSubrangeOfFile(file.getMetadata(), range.getFrom(), range.getTo()));
    }
  }

  /**
   * A {@link Coder} for {@link BoundedSource}s that wraps a {@link SerializableCoder}. We cannot
   * safely use an unwrapped SerializableCoder because {@link
   * SerializableCoder#structuralValue(Serializable)} assumes that coded elements support object
   * equality (https://issues.apache.org/jira/browse/BEAM-3807). By default, Coders compare equality
   * by serialized bytes, which we want in this case. It is usually safe to depend on coded
   * representation here because we only compare objects on bundle commit, which compares
   * serializations of the same object instance.
   *
   * <p>BoundedSources are generally not used as PCollection elements, so we do not expose this
   * coder for wider use.
   */
  public static class BoundedSourceCoder<T> extends CustomCoder<BoundedSource<T>> {
    private final Coder<BoundedSource<T>> coder;

    public BoundedSourceCoder() {
      coder = (Coder<BoundedSource<T>>) SerializableCoder.of((Class) BoundedSource.class);
    }

    @Override
    public void encode(BoundedSource<T> value, OutputStream outStream)
        throws CoderException, IOException {
      coder.encode(value, outStream);
    }

    @Override
    public BoundedSource<T> decode(InputStream inStream) throws CoderException, IOException {
      return coder.decode(inStream);
    }
  }

  /** Reads elements contained within an input {@link BoundedSource}. */
  // TODO: Extend to be a Splittable DoFn.
  public static class ReadFromBoundedSourceFn<T> extends DoFn<BoundedSource<T>, T> {
    @ProcessElement
    public void readSource(ProcessContext c) throws IOException {
      try (BoundedSource.BoundedReader<T> reader =
          c.element().createReader(c.getPipelineOptions())) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.outputWithTimestamp(reader.getCurrent(), reader.getCurrentTimestamp());
        }
      }
    }
  }
}
