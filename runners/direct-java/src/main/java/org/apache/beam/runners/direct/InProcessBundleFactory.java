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
package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

/**
 * A factory that produces bundles that perform no additional validation.
 */
class InProcessBundleFactory implements BundleFactory {
  public static InProcessBundleFactory create() {
    return new InProcessBundleFactory();
  }

  private InProcessBundleFactory() {}

  @Override
  public <T> UncommittedBundle<T> createRootBundle(PCollection<T> output) {
    return InProcessBundle.create(output, StructuralKey.of(null, VoidCoder.of()));
  }

  @Override
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output) {
    return InProcessBundle.create(output, input.getKey());
  }

  @Override
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, StructuralKey<K> key, PCollection<T> output) {
    return InProcessBundle.create(output, key);
  }

  /**
   * A {@link UncommittedBundle} that buffers elements in memory.
   */
  private static final class InProcessBundle<T> implements UncommittedBundle<T> {
    private final PCollection<T> pcollection;
    private final StructuralKey<?> key;
    private boolean committed = false;
    private ImmutableList.Builder<WindowedValue<T>> elements;

    /**
     * Create a new {@link InProcessBundle} for the specified {@link PCollection}.
     */
    public static <T> InProcessBundle<T> create(PCollection<T> pcollection, StructuralKey<?> key) {
      return new InProcessBundle<>(pcollection, key);
    }

    private InProcessBundle(PCollection<T> pcollection, StructuralKey<?> key) {
      this.pcollection = pcollection;
      this.key = key;
      this.elements = ImmutableList.builder();
    }

    @Override
    public PCollection<T> getPCollection() {
      return pcollection;
    }

    @Override
    public InProcessBundle<T> add(WindowedValue<T> element) {
      checkState(
          !committed,
          "Can't add element %s to committed bundle in PCollection %s",
          element,
          pcollection);
      elements.add(element);
      return this;
    }

    @Override
    public CommittedBundle<T> commit(final Instant synchronizedCompletionTime) {
      checkState(!committed, "Can't commit already committed bundle %s", this);
      committed = true;
      final Iterable<WindowedValue<T>> committedElements = elements.build();
      return new CommittedInProcessBundle<>(
          pcollection, key, committedElements, synchronizedCompletionTime);
    }
  }

  private static class CommittedInProcessBundle<T> implements CommittedBundle<T> {
    public CommittedInProcessBundle(
        PCollection<T> pcollection,
        StructuralKey<?> key,
        Iterable<WindowedValue<T>> committedElements,
        Instant synchronizedCompletionTime) {
      this.pcollection = pcollection;
      this.key = key;
      this.committedElements = committedElements;
      this.synchronizedCompletionTime = synchronizedCompletionTime;
    }

    private final PCollection<T> pcollection;
    /** The structural value key of the Bundle, as specified by the coder that created it. */
    private final StructuralKey<?> key;
    private final Iterable<WindowedValue<T>> committedElements;
    private final Instant synchronizedCompletionTime;

    @Override
    public StructuralKey<?> getKey() {
      return key;
    }

    @Override
    public Iterable<WindowedValue<T>> getElements() {
      return committedElements;
    }

    @Override
    public PCollection<T> getPCollection() {
      return pcollection;
    }

    @Override
    public Instant getSynchronizedProcessingOutputWatermark() {
      return synchronizedCompletionTime;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .omitNullValues()
          .add("pcollection", pcollection)
          .add("key", key)
          .add("elements", committedElements)
          .toString();
    }

    @Override
    public CommittedBundle<T> withElements(Iterable<WindowedValue<T>> elements) {
      return new CommittedInProcessBundle<>(
          pcollection, key, ImmutableList.copyOf(elements), synchronizedCompletionTime);
    }
  }
}
