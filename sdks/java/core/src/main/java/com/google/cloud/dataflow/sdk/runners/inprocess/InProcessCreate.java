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
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Create.Values;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * An in-process implementation of the {@link Values Create.Values} {@link PTransform}, implemented
 * using a {@link BoundedSource}.
 *
 * The coder is inferred via the {@link Values#getDefaultOutputCoder(PInput)} method on the original
 * transform.
 */
class InProcessCreate<T> extends ForwardingPTransform<PInput, PCollection<T>> {
  private final Create.Values<T> original;

  public static <T> InProcessCreate<T> from(Create.Values<T> original) {
    return new InProcessCreate<>(original);
  }

  private InProcessCreate(Values<T> original) {
    this.original = original;
  }

  @Override
  public PCollection<T> apply(PInput input) {
    Coder<T> elementCoder;
    try {
      elementCoder = original.getDefaultOutputCoder(input);
    } catch (CannotProvideCoderException e) {
      throw new IllegalArgumentException(
          "Unable to infer a coder and no Coder was specified. "
          + "Please set a coder by invoking Create.withCoder() explicitly.",
          e);
    }
    InMemorySource<T> source;
    try {
      source = new InMemorySource<>(original.getElements(), elementCoder);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    PCollection<T> result = input.getPipeline().apply(Read.from(source));
    result.setCoder(elementCoder);
    return result;
  }

  @Override
  public PTransform<PInput, PCollection<T>> delegate() {
    return original;
  }

  @VisibleForTesting
  static class InMemorySource<T> extends BoundedSource<T> {
    private final Collection<byte[]> allElementsBytes;
    private final long totalSize;
    private final Coder<T> coder;

    public InMemorySource(Iterable<T> elements, Coder<T> elemCoder)
        throws CoderException, IOException {
      allElementsBytes = new ArrayList<>();
      long totalSize = 0L;
      for (T element : elements) {
        byte[] bytes = CoderUtils.encodeToByteArray(elemCoder, element);
        allElementsBytes.add(bytes);
        totalSize += bytes.length;
      }
      this.totalSize = totalSize;
      this.coder = elemCoder;
    }

    /**
     * Create a new source with the specified bytes. The new source owns the input element bytes,
     * which must not be modified after this constructor is called.
     */
    private InMemorySource(Collection<byte[]> elementBytes, long totalSize, Coder<T> coder) {
      this.allElementsBytes = ImmutableList.copyOf(elementBytes);
      this.totalSize = totalSize;
      this.coder = coder;
    }

    @Override
    public List<? extends BoundedSource<T>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      ImmutableList.Builder<InMemorySource<T>> resultBuilder = ImmutableList.builder();
      long currentSourceSize = 0L;
      List<byte[]> currentElems = new ArrayList<>();
      for (byte[] elemBytes : allElementsBytes) {
        currentElems.add(elemBytes);
        currentSourceSize += elemBytes.length;
        if (currentSourceSize >= desiredBundleSizeBytes) {
          resultBuilder.add(new InMemorySource<>(currentElems, currentSourceSize, coder));
          currentElems.clear();
          currentSourceSize = 0L;
        }
      }
      if (!currentElems.isEmpty()) {
        resultBuilder.add(new InMemorySource<>(currentElems, currentSourceSize, coder));
      }
      return resultBuilder.build();
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return totalSize;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedSource.BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new BytesReader();
    }

    @Override
    public void validate() {}

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }

    private class BytesReader extends BoundedReader<T> {
      private final PeekingIterator<byte[]> iter;
      /**
       * Use an optional to distinguish between null next element (as Optional.absent()) and no next
       * element (next is null).
       */
      @Nullable private Optional<T> next;

      public BytesReader() {
        this.iter = Iterators.peekingIterator(allElementsBytes.iterator());
      }

      @Override
      public BoundedSource<T> getCurrentSource() {
        return InMemorySource.this;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        boolean hasNext = iter.hasNext();
        if (hasNext) {
          next = Optional.fromNullable(CoderUtils.decodeFromByteArray(coder, iter.next()));
        } else {
          next = null;
        }
        return hasNext;
      }

      @Override
      @Nullable
      public T getCurrent() throws NoSuchElementException {
        if (next == null) {
          throw new NoSuchElementException();
        }
        return next.orNull();
      }

      @Override
      public void close() throws IOException {}
    }
  }
}
