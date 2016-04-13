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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.OffsetBasedSource.OffsetBasedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
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

  /**
   * A {@link PTransformOverrideFactory} for {@link InProcessCreate}.
   */
  public static class InProcessCreateOverrideFactory implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
        PTransform<InputT, OutputT> transform) {
      if (transform instanceof Create.Values) {
        @SuppressWarnings("unchecked")
        PTransform<InputT, OutputT> override =
            (PTransform<InputT, OutputT>) from((Create.Values<OutputT>) transform);
        return override;
      }
      return transform;
    }
  }

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
      source = InMemorySource.fromIterable(original.getElements(), elementCoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
  static class InMemorySource<T> extends OffsetBasedSource<T> {
    private final List<byte[]> allElementsBytes;
    private final long totalSize;
    private final Coder<T> coder;

    public static <T> InMemorySource<T> fromIterable(Iterable<T> elements, Coder<T> elemCoder)
        throws CoderException, IOException {
      ImmutableList.Builder<byte[]> allElementsBytes = ImmutableList.builder();
      long totalSize = 0L;
      for (T element : elements) {
        byte[] bytes = CoderUtils.encodeToByteArray(elemCoder, element);
        allElementsBytes.add(bytes);
        totalSize += bytes.length;
      }
      return new InMemorySource<>(allElementsBytes.build(), totalSize, elemCoder);
    }

    /**
     * Create a new source with the specified bytes. The new source owns the input element bytes,
     * which must not be modified after this constructor is called.
     */
    private InMemorySource(List<byte[]> elementBytes, long totalSize, Coder<T> coder) {
      super(0, elementBytes.size(), 1);
      this.allElementsBytes = ImmutableList.copyOf(elementBytes);
      this.totalSize = totalSize;
      this.coder = coder;
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
      return new BytesReader<>(this);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return coder;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) throws Exception {
      return allElementsBytes.size();
    }

    @Override
    public OffsetBasedSource<T> createSourceForSubrange(long start, long end) {
      List<byte[]> primaryElems = allElementsBytes.subList((int) start, (int) end);
      long primarySizeEstimate =
          (long) (totalSize * primaryElems.size() / (double) allElementsBytes.size());
      return new InMemorySource<>(primaryElems, primarySizeEstimate, coder);
    }

    @Override
    public long getBytesPerOffset() {
      if (allElementsBytes.size() == 0) {
        return 0L;
      }
      return totalSize / allElementsBytes.size();
    }
  }

  private static class BytesReader<T> extends OffsetBasedReader<T> {
    private int index;
    /**
     * Use an optional to distinguish between null next element (as Optional.absent()) and no next
     * element (next is null).
     */
    @Nullable private Optional<T> next;

    public BytesReader(InMemorySource<T> source) {
      super(source);
      index = -1;
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

    @Override
    protected long getCurrentOffset() {
      return index;
    }

    @Override
    protected boolean startImpl() throws IOException {
      return advanceImpl();
    }

    @Override
    public synchronized InMemorySource<T> getCurrentSource() {
      return (InMemorySource<T>) super.getCurrentSource();
    }

    @Override
    protected boolean advanceImpl() throws IOException {
      InMemorySource<T> source = getCurrentSource();
      index++;
      if (index >= source.allElementsBytes.size()) {
        return false;
      }
      next =
          Optional.fromNullable(
              CoderUtils.decodeFromByteArray(
                  source.coder, source.allElementsBytes.get(index)));
      return true;
    }
  }
}
