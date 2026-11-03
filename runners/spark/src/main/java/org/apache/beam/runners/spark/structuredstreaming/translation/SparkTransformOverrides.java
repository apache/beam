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
package org.apache.beam.runners.spark.structuredstreaming.translation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.SplittableParDoNaiveBounded;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.sdk.util.construction.UnsupportedOverrideFactory;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/** {@link PTransform} overrides for Spark runner. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class SparkTransformOverrides {
  public static List<PTransformOverride> getDefaultOverrides(boolean streaming) {
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    // TODO: [https://github.com/apache/beam/issues/19107] Support @RequiresStableInput on Spark
    // runner
    builder.add(
        PTransformOverride.of(
            PTransformMatchers.requiresStableInputParDoMulti(),
            UnsupportedOverrideFactory.withMessage(
                "Spark runner currently doesn't support @RequiresStableInput annotation.")));
    if (!streaming) {
      builder
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.urnEqualTo(PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                  new SplittableParDoNaiveBounded.OverrideFactory()));
    } else {
      builder
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(SplittableParDo.PrimitiveBoundedRead.class),
                  new BoundedToUnboundedReadOverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Impulse.class), new ImpulseOverrideFactory()));
    }
    return builder.build();
  }

  private static class BoundedToUnboundedReadOverrideFactory<T>
      implements PTransformOverrideFactory<
          PBegin, PCollection<T>, SplittableParDo.PrimitiveBoundedRead<T>> {
    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, SplittableParDo.PrimitiveBoundedRead<T>>
            transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(),
          new StreamingBoundedRead<>(transform.getTransform().getSource()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class ImpulseOverrideFactory
      implements PTransformOverrideFactory<PBegin, PCollection<byte[]>, Impulse> {
    @Override
    public PTransformReplacement<PBegin, PCollection<byte[]>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<byte[]>, Impulse> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingBoundedRead<>(new ImpulseSource()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<byte[]> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Adapts a {@link BoundedSource} to an unbounded read while keeping the output bounded. {@link
   * SplittableParDo.PrimitiveUnboundedRead} is applied directly because the {@link Read} to
   * primitive conversion runs before {@code replaceTransforms}.
   */
  private static class StreamingBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    StreamingBoundedRead(BoundedSource<T> source) {
      this.source = source;
      source.validate();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      return Pipeline.applyTransform(
              input,
              new SplittableParDo.PrimitiveUnboundedRead<>(
                  Read.from(new BoundedToUnboundedSourceAdapter<>(source))))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
  }

  /** Single element {@link BoundedSource} backing {@link Impulse} in streaming mode. */
  private static final class ImpulseSource extends BoundedSource<byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public List<? extends BoundedSource<byte[]>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 1L;
    }

    @Override
    public BoundedReader<byte[]> createReader(PipelineOptions options) {
      return new ImpulseReader(this);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<byte[]> getOutputCoder() {
      return ByteArrayCoder.of();
    }
  }

  private static final class ImpulseReader extends BoundedSource.BoundedReader<byte[]> {
    private final ImpulseSource source;
    private boolean done;

    ImpulseReader(ImpulseSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      if (done) {
        return false;
      }
      done = true;
      return true;
    }

    @Override
    public boolean advance() {
      return false;
    }

    @Override
    public byte[] getCurrent() {
      return new byte[0];
    }

    @Override
    public Instant getCurrentTimestamp() {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public void close() {}

    @Override
    public BoundedSource<byte[]> getCurrentSource() {
      return source;
    }
  }
}
