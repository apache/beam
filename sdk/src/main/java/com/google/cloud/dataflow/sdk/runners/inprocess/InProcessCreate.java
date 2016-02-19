/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Create.Values;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An in memory implementation of the {@link Values Create.Values} {@link PTransform}, implemented'
 * using a {@link BoundedSource}.
 *
 * The coder is inferred via the {@link Values#getDefaultOutputCoder(PInput)} method on the original
 * transform.
 */
class InProcessCreate<T> extends PTransform<PInput, PCollection<T>> {
  private final Create.Values<T> original;
  private final InMemorySource<T> source;

  public static <T> InProcessCreate<T> from(Create.Values<T> original) {
    return new InProcessCreate<>(original);
  }

  private InProcessCreate(Values<T> original) {
    this.original = original;
    this.source = new InMemorySource<>(original.getElements());
  }

  @Override
  public PCollection<T> apply(PInput input) {
    input.getPipeline().getCoderRegistry();
    PCollection<T> result = input.getPipeline().apply(Read.from(source));
    try {
      result.setCoder(original.getDefaultOutputCoder(input));
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException("Unable to infer a coder and no Coder was specified. "
          + "Please set a coder by invoking Create.withCoder() explicitly.", e);
    }
    return result;
  }

  private static class InMemorySource<T> extends BoundedSource<T> {
    private final Iterable<T> elements;

    public InMemorySource(Iterable<T> elements) {
      this.elements = elements;
    }

    @Override
    public List<? extends BoundedSource<T>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0L;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new IterableReader();
    }

    @Override
    public void validate() {}

    @Override
    public Coder<T> getDefaultOutputCoder() {
      // Return a coder that exclusively throws exceptions. The coder is set properly in apply, or
      // an illegal argument exception is thrown.
      return new StandardCoder<T>() {
        @Override
        public void encode(T value, OutputStream outStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
          throw new CoderException("Default Create Coder cannot be used");
        }

        @Override
        public T decode(
            InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
          throw new CoderException("Default Create Coder cannot be used");
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
          return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic()
            throws com.google.cloud.dataflow.sdk.coders.Coder.NonDeterministicException {
          throw new NonDeterministicException(
              this, Collections.<String>singletonList("Default Create Coder cannot be used"));
        }
      };
    }

    private class IterableReader extends BoundedReader<T> {
      private final PeekingIterator<T> iter;

      public IterableReader() {
        this.iter = Iterators.peekingIterator(elements.iterator());
      }

      @Override
      public BoundedSource<T> getCurrentSource() {
        return InMemorySource.this;
      }

      @Override
      public boolean start() throws IOException {
        return iter.hasNext();
      }

      @Override
      public boolean advance() throws IOException {
        iter.next();
        return iter.hasNext();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return iter.peek();
      }

      @Override
      public void close() throws IOException {}
    }
  }
}
