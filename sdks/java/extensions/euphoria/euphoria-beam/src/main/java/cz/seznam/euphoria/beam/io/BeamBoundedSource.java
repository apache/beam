/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link BoundedSource} created from {@link BoundedDataSource}.
 */
public class BeamBoundedSource<T> extends BoundedSource<T> {
  
  private final BoundedDataSource<T> wrap;

  private BeamBoundedSource(BoundedDataSource<T> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions po) throws Exception {
    return wrap.split(desiredBundleSizeBytes)
        .stream()
        .map(BeamBoundedSource::wrap)
        .collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
    // not supported
    return -1L;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions po) throws IOException {
    final cz.seznam.euphoria.core.client.io.BoundedReader<T> reader = wrap.openReader();
    return new BoundedReader<T>() {

      private T current = null;

      @Override
      public BoundedSource<T> getCurrentSource() {
        return BeamBoundedSource.this;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        final boolean ret = reader.hasNext();
        if (ret) {
          current = reader.next();
        }
        return ret;
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    };
  }

  @Override
  public void validate() {
    // FIXME
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return new KryoCoder<>();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BeamBoundedSource && ((BeamBoundedSource) obj).wrap.equals(this.wrap);
  }

  @Override
  public int hashCode() {
    return wrap.hashCode();
  }

  public static <T> BeamBoundedSource<T> wrap(BoundedDataSource<T> wrap) {
    return new BeamBoundedSource<>(wrap);
  }

}
