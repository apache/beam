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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Read from a Java {@link BoundedSource} via the {@link Impulse} and {@link ParDo} primitive
 * transforms.
 */
public class JavaReadViaImpulse {
  private static final long DEFAULT_BUNDLE_SIZE = 64L << 20;

  public static <T> PTransform<PBegin, PCollection<T>> bounded(BoundedSource<T> source) {
    return new BoundedReadViaImpulse<>(source);
  }

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
          .setCoder((Coder<BoundedSource<T>>) SerializableCoder.of((Class) BoundedSource.class))
          .apply(Reshuffle.viaRandomKey())
          .apply(ParDo.of(new ReadFromBoundedSourceFn<>()))
          .setCoder(source.getOutputCoder());
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
}
