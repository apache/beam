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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.BundleSplit;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

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
    return InProcessBundle.unkeyed(output);
  }

  @Override
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output) {
    return input.isKeyed()
        ? InProcessBundle.keyed(output, input.getKey())
        : InProcessBundle.unkeyed(output);
  }

  @Override
  public <T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, Object key, PCollection<T> output) {
    return InProcessBundle.keyed(output, key);
  }

  /**
   * A {@link UncommittedBundle} that buffers elements in memory.
   */
  private static final class InProcessBundle<T> implements UncommittedBundle<T> {
    private final PCollection<T> pcollection;
    private final boolean keyed;
    private final Object key;
    private boolean committed = false;
    private ImmutableList.Builder<WindowedValue<T>> elements;

    /**
     * Create a new {@link InProcessBundle} for the specified {@link PCollection} without a key.
     */
    public static <T> InProcessBundle<T> unkeyed(PCollection<T> pcollection) {
      return new InProcessBundle<T>(pcollection, false, null);
    }

    /**
     * Create a new {@link InProcessBundle} for the specified {@link PCollection} with the specified
     * key.
     *
     * <p>See {@link CommittedBundle#getKey()} and {@link CommittedBundle#isKeyed()} for more
     * information.
     */
    public static <T> InProcessBundle<T> keyed(PCollection<T> pcollection, Object key) {
      return new InProcessBundle<T>(pcollection, true, key);
    }

    private InProcessBundle(PCollection<T> pcollection, boolean keyed, Object key) {
      this.pcollection = pcollection;
      this.keyed = keyed;
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
      final ImmutableList<WindowedValue<T>> committedElements = elements.build();
      return createCommittedBundle(committedElements, synchronizedCompletionTime);
    }

    private CommittedBundle<T> createCommittedBundle(
        List<WindowedValue<T>> elements, final Instant synchronizedCompletionTime) {
      final List<WindowedValue<T>> committedElements = ImmutableList.copyOf(elements);
      return new CommittedBundle<T>() {
        @Override
        @Nullable
        public Object getKey() {
          return key;
        }

        @Override
        public boolean isKeyed() {
          return keyed;
        }

        @Override
        public Iterable<WindowedValue<T>> getElements() {
          return committedElements;
        }

        @Override
        public BundleSplit<T> splitOffElements(Iterable<WindowedValue<T>> elements) {
          if (Iterables.isEmpty(elements)) {
            return new SimpleBundleSplit<>(
                this,
                createCommittedBundle(
                    ImmutableList.<WindowedValue<T>>of(), synchronizedCompletionTime));
          }

          List<WindowedValue<T>> primaryElements = new ArrayList<>();
          ImmutableList.Builder<WindowedValue<T>> residualElements = ImmutableList.builder();
          primaryElements.addAll(committedElements);
          for (WindowedValue<T> element : elements) {
            if (!primaryElements.remove(element)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Tried to split off element %s from a committed bundle,"
                          + " but that element is not contained in this bundle.",
                      element));
            }
            residualElements.add(element);
          }
          return new SimpleBundleSplit<>(
              createCommittedBundle(primaryElements, synchronizedCompletionTime),
              createCommittedBundle(residualElements.build(), synchronizedCompletionTime));
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
      };
    }
  }

  private static class SimpleBundleSplit<T> implements BundleSplit<T> {
    private final CommittedBundle<T> primary;
    private final CommittedBundle<T> residual;

    public SimpleBundleSplit(CommittedBundle<T> primary, CommittedBundle<T> residual) {
      this.primary = primary;
      this.residual = residual;
    }

    @Override
    public CommittedBundle<T> getPrimary() {
      return primary;
    }

    @Override
    public CommittedBundle<T> getResidual() {
      return residual;
    }
  }
}
