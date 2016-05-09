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

import static org.apache.beam.sdk.util.StringUtils.approximateSimpleName;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;

import com.google.api.client.util.Lists;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import autovalue.shaded.com.google.common.common.annotations.VisibleForTesting;

/**
 * {@link PTransform} that performs a unbounded read from an {@link BoundedSource}.
 *
 * <p>Created by {@link Read}.
 */
public class UnboundedReadFromBoundedSource<T> extends PTransform<PInput, PCollection<T>> {
  private final BoundedSource<T> source;

  UnboundedReadFromBoundedSource(BoundedSource<T> source) {
    this.source = source;
  }

  @Override
  public PCollection<T> apply(PInput input) {
    return Pipeline.applyTransform(input,
        Read.from(new BoundedToUnboundedSourceAdapter<>(source)));
  }

  @Override
  protected Coder<T> getDefaultOutputCoder() {
    return source.getDefaultOutputCoder();
  }

  @Override
  public String getKindString() {
    return "Read(" + approximateSimpleName(source.getClass()) + ")";
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // We explicitly do not register base-class data, instead we use the delegate inner source.
    builder
        .add(DisplayData.item("source", source.getClass()))
        .include(source);
  }

  @VisibleForTesting
  static class BoundedToUnboundedSourceAdapter<T>
      extends UnboundedSource<T, BoundedToUnboundedSourceAdapter.Checkpoint> {

    private final BoundedSource<T> boundedSource;

    public BoundedToUnboundedSourceAdapter(BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public void validate() {
      boundedSource.validate();
    }

    @Override
    public List<BoundedToUnboundedSourceAdapter<T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      long desiredBundleSize = boundedSource.getEstimatedSizeBytes(options) / desiredNumSplits;
      List<BoundedToUnboundedSourceAdapter<T>> result = Lists.newArrayList();
      for (BoundedSource<T> split : boundedSource.splitIntoBundles(desiredBundleSize, options)) {
        result.add(new BoundedToUnboundedSourceAdapter<>(split));
      }
      return result;
    }

    @Override
    public Reader createReader(PipelineOptions options, Checkpoint checkpoint) throws IOException {
      return new Reader(boundedSource.createReader(options), checkpoint);
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return boundedSource.getDefaultOutputCoder();
    }

    static class Checkpoint implements UnboundedSource.CheckpointMark {
      private final boolean done;

      public Checkpoint(boolean done) {
        this.done = done;
      }

      public boolean isDone() {
        return done;
      }

      @Override
      public void finalizeCheckpoint() {}
    }

    @Override
    public Coder<Checkpoint> getCheckpointMarkCoder() {
      return AvroCoder.of(Checkpoint.class);
    }

    private class Reader extends UnboundedReader<T> {
      private final BoundedSource.BoundedReader<T> boundedReader;
      private boolean done;

      public Reader(BoundedSource.BoundedReader<T> boundedReader, Checkpoint checkpoint) {
        this.done = checkpoint != null && checkpoint.isDone();
        this.boundedReader = boundedReader;
      }

      @Override
      public boolean start() throws IOException {
        if (done) {
          return false;
        }
        boolean result = boundedReader.start();
        if (!result) {
          done = true;
          boundedReader.close();
        }
        return result;
      }

      @Override
      public boolean advance() throws IOException {
        if (done) {
          return false;
        }
        boolean result = boundedReader.advance();
        if (!result) {
          done = true;
          boundedReader.close();
        }
        return result;
      }

      @Override
      public void close() {}

      @Override
      public T getCurrent() throws NoSuchElementException {
        return boundedReader.getCurrent();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return boundedReader.getCurrentTimestamp();
      }

      @Override
      public Instant getWatermark() {
        return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      @Override
      public Checkpoint getCheckpointMark() {
        return new Checkpoint(done);
      }

      @Override
      public BoundedToUnboundedSourceAdapter<T> getCurrentSource() {
        return BoundedToUnboundedSourceAdapter.this;
      }
    }
  }
}
