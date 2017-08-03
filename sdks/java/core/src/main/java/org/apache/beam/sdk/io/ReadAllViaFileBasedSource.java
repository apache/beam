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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads each file in the input {@link PCollection} of {@link Metadata} using given parameters for
 * splitting files into offset ranges and for creating a {@link FileBasedSource} for a file. The
 * input {@link PCollection} must not contain {@link ResourceId#isDirectory directories}.
 *
 * <p>To obtain the collection of {@link Metadata} from a filepattern, use {@link
 * Match#filepatterns()}.
 */
class ReadAllViaFileBasedSource<T> extends PTransform<PCollection<Metadata>, PCollection<T>> {
  private final SerializableFunction<String, Boolean> isSplittable;
  private final long desiredBundleSizeBytes;
  private final SerializableFunction<String, FileBasedSource<T>> createSource;

  public ReadAllViaFileBasedSource(
      SerializableFunction<String, Boolean> isSplittable,
      long desiredBundleSizeBytes,
      SerializableFunction<String, FileBasedSource<T>> createSource) {
    this.isSplittable = isSplittable;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.createSource = createSource;
  }

  @Override
  public PCollection<T> expand(PCollection<Metadata> input) {
    return input
        .apply(
            "Split into ranges",
            ParDo.of(new SplitIntoRangesFn(isSplittable, desiredBundleSizeBytes)))
        .apply("Reshuffle", new ReshuffleWithUniqueKey<KV<Metadata, OffsetRange>>())
        .apply("Read ranges", ParDo.of(new ReadFileRangesFn<T>(createSource)));
  }

  private static class ReshuffleWithUniqueKey<T>
      extends PTransform<PCollection<T>, PCollection<T>> {
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          .apply("Unique key", ParDo.of(new AssignUniqueKeyFn<T>()))
          .apply("Reshuffle", Reshuffle.<Integer, T>of())
          .apply("Values", Values.<T>create());
    }
  }

  private static class AssignUniqueKeyFn<T> extends DoFn<T, KV<Integer, T>> {
    private int index;

    @Setup
    public void setup() {
      this.index = ThreadLocalRandom.current().nextInt();
    }

    @ProcessElement
    public void process(ProcessContext c) {
      c.output(KV.of(++index, c.element()));
    }
  }

  private static class SplitIntoRangesFn extends DoFn<Metadata, KV<Metadata, OffsetRange>> {
    private final SerializableFunction<String, Boolean> isSplittable;
    private final long desiredBundleSizeBytes;

    private SplitIntoRangesFn(
        SerializableFunction<String, Boolean> isSplittable, long desiredBundleSizeBytes) {
      this.isSplittable = isSplittable;
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      Metadata metadata = c.element();
      checkArgument(
          !metadata.resourceId().isDirectory(),
          "Resource %s is a directory",
          metadata.resourceId());
      if (!metadata.isReadSeekEfficient()
          || !isSplittable.apply(metadata.resourceId().toString())) {
        c.output(KV.of(metadata, new OffsetRange(0, metadata.sizeBytes())));
        return;
      }
      for (OffsetRange range :
          new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSizeBytes, 0)) {
        c.output(KV.of(metadata, range));
      }
    }
  }

  private static class ReadFileRangesFn<T> extends DoFn<KV<Metadata, OffsetRange>, T> {
    private final SerializableFunction<String, FileBasedSource<T>> createSource;

    private ReadFileRangesFn(SerializableFunction<String, FileBasedSource<T>> createSource) {
      this.createSource = createSource;
    }

    @ProcessElement
    public void process(ProcessContext c) throws IOException {
      Metadata metadata = c.element().getKey();
      OffsetRange range = c.element().getValue();
      FileBasedSource<T> source = createSource.apply(metadata.toString());
      try (BoundedSource.BoundedReader<T> reader =
          source
              .createForSubrangeOfFile(metadata, range.getFrom(), range.getTo())
              .createReader(c.getPipelineOptions())) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(reader.getCurrent());
        }
      }
    }
  }
}
