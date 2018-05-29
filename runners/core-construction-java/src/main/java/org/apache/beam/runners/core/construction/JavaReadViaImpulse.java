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

package org.apache.beam.runners.core.construction;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Read from a Java {@link BoundedSource} via the {@link Impulse} and {@link ParDo} primitive
 * transforms.
 */
public class JavaReadViaImpulse {
  private static final long DEFAULT_BUNDLE_SIZE = 64L << 20;

  public static <T> PTransform<PBegin, PCollection<T>> bounded(BoundedSource<T> source) {
    return new BoundedReadViaImpulse<>(source);
  }

  public static PTransformOverride boundedOverride() {
    return PTransformOverride.of(boundedMatcher(), new BoundedOverrideFactory<>());
  }

  private static PTransformMatcher boundedMatcher() {
    return PTransformMatchers.urnEqualTo(PTransformTranslation.READ_TRANSFORM_URN)
        .and(transform ->
            ReadTranslation.sourceIsBounded(transform) == PCollection.IsBounded.BOUNDED);
  }

  // TODO: https://issues.apache.org/jira/browse/BEAM-3859 Support unbounded reads via impulse.

  private static class BoundedReadViaImpulse<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    private BoundedReadViaImpulse(BoundedSource<T> source) {
      this.source = source;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input
          .apply(Impulse.create())
          .apply(ParDo.of(new SplitBoundedSourceFn<>(source, DEFAULT_BUNDLE_SIZE)))
          .setCoder(new BoundedSourceCoder<>())
          .apply(Reshuffle.viaRandomKey())
          .apply(ParDo.of(new ReadFromBoundedSourceFn<>()))
          .setCoder(source.getOutputCoder());
    }
  }

  private static class BoundedOverrideFactory<T> implements PTransformOverrideFactory<
      PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform) {
      PBegin input = PBegin.in(transform.getPipeline());
      BoundedSource<T> source;
      try {
        source = ReadTranslation.boundedSourceFromTransform(transform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return PTransformReplacement.of(input, bounded(source));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(Map<TupleTag<?>, PValue> outputs,
                                                     PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }

  }

  @VisibleForTesting
  static class SplitBoundedSourceFn<T> extends DoFn<byte[], BoundedSource<T>> {
    private final BoundedSource<T> source;
    private final long bundleSize;

    public SplitBoundedSourceFn(BoundedSource<T> source, long bundleSize) {
      this.source = source;
      this.bundleSize = bundleSize;
    }

    @ProcessElement
    public void splitSource(ProcessContext ctxt) throws Exception {
      for (BoundedSource<T> split : source.split(bundleSize, ctxt.getPipelineOptions())) {
        ctxt.output(split);
      }
    }
  }

  /** Reads elements contained within an input {@link BoundedSource}. */
  // TODO: Extend to be a Splittable DoFn.
  @VisibleForTesting
  static class ReadFromBoundedSourceFn<T> extends DoFn<BoundedSource<T>, T> {
    @ProcessElement
    public void readSoruce(ProcessContext ctxt) throws IOException {
      BoundedSource.BoundedReader<T> reader =
          ctxt.element().createReader(ctxt.getPipelineOptions());
      for (boolean more = reader.start(); more; more = reader.advance()) {
        ctxt.outputWithTimestamp(reader.getCurrent(), reader.getCurrentTimestamp());
      }
    }
  }

  /**
   * A {@link Coder} for {@link BoundedSource}s that wraps a {@link SerializableCoder}. We cannot
   * safely use an unwrapped SerializableCoder because
   * {@link SerializableCoder#structuralValue(Serializable)} assumes that coded elements support
   * object equality (https://issues.apache.org/jira/browse/BEAM-3807). By default, Coders compare
   * equality by serialized bytes, which we want in this case. It is usually safe to depend on coded
   * representation here because we only compare objects on bundle commit, which compares
   * serializations of the same object instance.
   *
   * <p>BoundedSources are generally not used as PCollection elements, so we do not expose this
   * coder for wider use.
   */
  @VisibleForTesting
  static class BoundedSourceCoder<T> extends CustomCoder<BoundedSource<T>> {
    private final Coder<BoundedSource<T>> coder;

    BoundedSourceCoder() {
      coder = (Coder<BoundedSource<T>>) SerializableCoder.of((Class) BoundedSource.class);
    }

    @Override
    public void encode(BoundedSource<T> value, OutputStream outStream) throws CoderException,
        IOException {
      coder.encode(value, outStream);
    }

    @Override
    public BoundedSource<T> decode(InputStream inStream) throws CoderException, IOException {
      return coder.decode(inStream);
    }

  }
}
