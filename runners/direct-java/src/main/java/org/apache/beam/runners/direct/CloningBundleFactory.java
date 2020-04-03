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

import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.Instant;

/**
 * A {@link BundleFactory} where a created {@link UncommittedBundle} clones all elements added to it
 * using the coder of the {@link PCollection}.
 */
class CloningBundleFactory implements BundleFactory {
  private static final CloningBundleFactory INSTANCE = new CloningBundleFactory();

  public static CloningBundleFactory create() {
    return INSTANCE;
  }

  private final ImmutableListBundleFactory underlying;

  private CloningBundleFactory() {
    this.underlying = ImmutableListBundleFactory.create();
  }

  @Override
  public <T> UncommittedBundle<T> createRootBundle() {
    // The DirectRunner is responsible for these elements, but they need not be encodable.
    return underlying.createRootBundle();
  }

  @Override
  public <T> UncommittedBundle<T> createBundle(PCollection<T> output) {
    return new CloningBundle<>(underlying.createBundle(output));
  }

  @Override
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      StructuralKey<K> key, PCollection<T> output) {
    return new CloningBundle<>(underlying.createKeyedBundle(key, output));
  }

  private static class CloningBundle<T> implements UncommittedBundle<T> {
    private final UncommittedBundle<T> underlying;
    private final Coder<T> coder;

    private CloningBundle(UncommittedBundle<T> underlying) {
      this.underlying = underlying;
      this.coder = underlying.getPCollection().getCoder();
    }

    @Override
    public PCollection<T> getPCollection() {
      return underlying.getPCollection();
    }

    @Override
    public UncommittedBundle<T> add(WindowedValue<T> element) {
      try {
        // Use the cloned value to ensure that if the coder behaves poorly (e.g. a NoOpCoder that
        // does not expect to be used) that is reflected in the values given to downstream
        // transforms
        WindowedValue<T> clone = element.withValue(CoderUtils.clone(coder, element.getValue()));
        underlying.add(clone);
      } catch (CoderException e) {
        throw UserCodeException.wrap(e);
      }
      return this;
    }

    @Override
    public CommittedBundle<T> commit(Instant synchronizedProcessingTime) {
      return underlying.commit(synchronizedProcessingTime);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("Data", underlying.toString())
          .add("Coder", coder.toString())
          .toString();
    }
  }
}
