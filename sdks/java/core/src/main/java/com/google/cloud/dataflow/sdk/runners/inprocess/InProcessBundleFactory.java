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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

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
      final Iterable<WindowedValue<T>> committedElements = elements.build();
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
}

