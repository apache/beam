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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * A factory that produces bundles that perform no additional validation.
 */
class ImmutableListBundleFactory implements BundleFactory {
  public static ImmutableListBundleFactory create() {
    return new ImmutableListBundleFactory();
  }

  private ImmutableListBundleFactory() {}

  @Override
  public <T> UncommittedBundle<T> createRootBundle() {
    return UncommittedImmutableListBundle.create(null, StructuralKey.empty());
  }

  @Override
  public <T> UncommittedBundle<T> createBundle(PCollection<T> output) {
    return UncommittedImmutableListBundle.create(output, StructuralKey.empty());
  }

  @Override
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      StructuralKey<K> key, PCollection<T> output) {
    return UncommittedImmutableListBundle.create(output, key);
  }

  /**
   * A {@link UncommittedBundle} that buffers elements in memory.
   */
  private static final class UncommittedImmutableListBundle<T> implements UncommittedBundle<T> {
    private final PCollection<T> pcollection;
    private final StructuralKey<?> key;
    private boolean committed = false;
    private ImmutableList.Builder<WindowedValue<T>> elements;

    /**
     * Create a new {@link UncommittedImmutableListBundle} for the specified {@link PCollection}.
     */
    public static <T> UncommittedImmutableListBundle<T> create(
        PCollection<T> pcollection,
        StructuralKey<?> key) {
      return new UncommittedImmutableListBundle<>(pcollection, key);
    }

    private UncommittedImmutableListBundle(PCollection<T> pcollection, StructuralKey<?> key) {
      this.pcollection = pcollection;
      this.key = key;
      this.elements = ImmutableList.builder();
    }

    @Override
    public PCollection<T> getPCollection() {
      return pcollection;
    }

    @Override
    public UncommittedImmutableListBundle<T> add(WindowedValue<T> element) {
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
      return CommittedImmutableListBundle.create(
          pcollection, key, committedElements, synchronizedCompletionTime);
    }
  }

  @AutoValue
  abstract static class CommittedImmutableListBundle<T> implements CommittedBundle<T> {
    public static <T> CommittedImmutableListBundle<T> create(
        @Nullable PCollection<T> pcollection,
        StructuralKey<?> key,
        Iterable<WindowedValue<T>> committedElements,
        Instant synchronizedCompletionTime) {
      return new AutoValue_ImmutableListBundleFactory_CommittedImmutableListBundle(
          pcollection, key, committedElements, synchronizedCompletionTime);
    }

    @Override
    public CommittedBundle<T> withElements(Iterable<WindowedValue<T>> elements) {
      return create(
          getPCollection(),
          getKey(),
          ImmutableList.copyOf(elements),
          getSynchronizedProcessingOutputWatermark());
    }
  }
}
