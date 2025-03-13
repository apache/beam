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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.direct.DirectRunner.Enforcement;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.util.MutationDetector;
import org.apache.beam.sdk.util.MutationDetectors;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.SetMultimap;
import org.joda.time.Instant;

/**
 * A {@link BundleFactory} that ensures that elements added to it are not mutated after being
 * output. Immutability checks are enforced at the time {@link UncommittedBundle#commit(Instant)} is
 * called, checking the value at that time against the value at the time the element was added. All
 * elements added to the bundle will be encoded by the {@link Coder} of the underlying {@link
 * PCollection}.
 *
 * <p>This catches errors during the execution of a {@link DoFn} caused by modifying an element
 * after it is added to an output {@link PCollection}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ImmutabilityCheckingBundleFactory implements BundleFactory {
  /**
   * Create a new {@link ImmutabilityCheckingBundleFactory} that uses the underlying {@link
   * BundleFactory} to create the output bundle.
   */
  public static ImmutabilityCheckingBundleFactory create(
      BundleFactory underlying, DirectGraph graph) {
    return new ImmutabilityCheckingBundleFactory(underlying, graph);
  }

  private final BundleFactory underlying;
  private final DirectGraph graph;

  private ImmutabilityCheckingBundleFactory(BundleFactory underlying, DirectGraph graph) {
    this.underlying = checkNotNull(underlying);
    this.graph = graph;
  }

  /**
   * {@inheritDoc}.
   *
   * @return a root bundle created by the underlying {@link PCollection}. Root bundles belong to the
   *     runner, which is required to use the contents in a way that is mutation-safe.
   */
  @Override
  public <T> UncommittedBundle<T> createRootBundle() {
    return underlying.createRootBundle();
  }

  @Override
  public <T> UncommittedBundle<T> createBundle(PCollection<T> output) {
    if (Enforcement.IMMUTABILITY.appliesTo(output, graph)) {
      return new ImmutabilityEnforcingBundle<>(underlying.createBundle(output));
    }
    return underlying.createBundle(output);
  }

  @Override
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      StructuralKey<K> key, PCollection<T> output) {
    if (Enforcement.IMMUTABILITY.appliesTo(output, graph)) {
      return new ImmutabilityEnforcingBundle<>(underlying.createKeyedBundle(key, output));
    }
    return underlying.createKeyedBundle(key, output);
  }

  private class ImmutabilityEnforcingBundle<T> implements UncommittedBundle<T> {
    private final UncommittedBundle<T> underlying;
    private final SetMultimap<WindowedValue<T>, MutationDetector> mutationDetectors;
    private Coder<T> coder;

    public ImmutabilityEnforcingBundle(UncommittedBundle<T> underlying) {
      this.underlying = underlying;
      mutationDetectors = HashMultimap.create();
      coder = getPCollection().getCoder();
    }

    @Override
    public PCollection<T> getPCollection() {
      return underlying.getPCollection();
    }

    @Override
    public UncommittedBundle<T> add(WindowedValue<T> element) {
      try {
        mutationDetectors.put(
            element, MutationDetectors.forValueWithCoder(element.getValue(), coder));
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
      underlying.add(element);
      return this;
    }

    @Override
    public CommittedBundle<T> commit(Instant synchronizedProcessingTime) {
      for (MutationDetector detector : mutationDetectors.values()) {
        try {
          detector.verifyUnmodified();
        } catch (IllegalMutationException exn) {
          throw new IllegalMutationException(
              String.format(
                  "PTransform %s mutated value %s after it was output (new value was %s)."
                      + " Values must not be mutated in any way after being output.",
                  graph.getProducer(underlying.getPCollection()).getFullName(),
                  exn.getSavedValue(),
                  exn.getNewValue()),
              exn.getSavedValue(),
              exn.getNewValue(),
              exn);
        }
      }
      return underlying.commit(synchronizedProcessingTime);
    }
  }
}
