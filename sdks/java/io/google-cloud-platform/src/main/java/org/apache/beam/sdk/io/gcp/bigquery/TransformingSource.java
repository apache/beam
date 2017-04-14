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

package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * A {@link BoundedSource} that reads from {@code BoundedSource<T>}
 * and transforms elements to type {@code V}.
*/
@VisibleForTesting
class TransformingSource<T, V> extends BoundedSource<V> {
  private final BoundedSource<T> boundedSource;
  private final SerializableFunction<T, V> function;
  private final Coder<V> outputCoder;

  TransformingSource(
      BoundedSource<T> boundedSource,
      SerializableFunction<T, V> function,
      Coder<V> outputCoder) {
    this.boundedSource = checkNotNull(boundedSource, "boundedSource");
    this.function = checkNotNull(function, "function");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
  }

  @Override
  public List<? extends BoundedSource<V>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    return Lists.transform(
        boundedSource.split(desiredBundleSizeBytes, options),
        new Function<BoundedSource<T>, BoundedSource<V>>() {
          @Override
          public BoundedSource<V> apply(BoundedSource<T> input) {
            return new TransformingSource<>(input, function, outputCoder);
          }
        });
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return boundedSource.getEstimatedSizeBytes(options);
  }

  @Override
  public BoundedReader<V> createReader(PipelineOptions options) throws IOException {
    return new TransformingReader(boundedSource.createReader(options));
  }

  @Override
  public void validate() {
    boundedSource.validate();
  }

  @Override
  public Coder<V> getDefaultOutputCoder() {
    return outputCoder;
  }

  private class TransformingReader extends BoundedReader<V> {
    private final BoundedReader<T> boundedReader;

    private TransformingReader(BoundedReader<T> boundedReader) {
      this.boundedReader = checkNotNull(boundedReader, "boundedReader");
    }

    @Override
    public synchronized BoundedSource<V> getCurrentSource() {
      return new TransformingSource<>(boundedReader.getCurrentSource(), function, outputCoder);
    }

    @Override
    public boolean start() throws IOException {
      return boundedReader.start();
    }

    @Override
    public boolean advance() throws IOException {
      return boundedReader.advance();
    }

    @Override
    public V getCurrent() throws NoSuchElementException {
      T current = boundedReader.getCurrent();
      return function.apply(current);
    }

    @Override
    public void close() throws IOException {
      boundedReader.close();
    }

    @Override
    public synchronized BoundedSource<V> splitAtFraction(double fraction) {
      BoundedSource<T> split = boundedReader.splitAtFraction(fraction);
      return split == null ? null : new TransformingSource<>(split, function, outputCoder);
    }

    @Override
    public Double getFractionConsumed() {
      return boundedReader.getFractionConsumed();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return boundedReader.getCurrentTimestamp();
    }
  }
}
