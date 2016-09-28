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

import java.util.Collections;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * A {@link CommittedBundle} that provides an impulse to run a root {@link PTransform}. An
 * {@link ImpulseBundle} contains no elements and belongs to no {@link PCollection}.
 *
 * <p>{@link ImpulseBundle} instances have only reference equality.
 */
final class ImpulseBundle implements CommittedBundle<Object> {
  /**
   * Returns a new {@link ImpulseBundle}.
   */
  public static CommittedBundle<?> create() {
    return new ImpulseBundle();
  }

  /**
   * {@inheritDoc}
   *
   * <p>An {@link ImpulseBundle} does not belong to any {@link PCollection}. The
   * {@link DirectRunner} should assign these bundles directly to a {@link PTransform} application.
   *
   * @throws IllegalStateException always
   */
  @Override
  public PCollection<Object> getPCollection() {
    throw new IllegalStateException(String.format("An %s does not belong to any PCollection",
        getClass().getSimpleName()));
  }

  /**
   * {@inheritDoc}
   *
   * @return the empty key
   */
  @Override
  public StructuralKey<?> getKey() {
    return StructuralKey.of(null, VoidCoder.of());
  }

  /**
   * {@inheritDoc}
   *
   * @return an empty {@link Iterable}.
   */
  @Override
  public Iterable<WindowedValue<Object>> getElements() {
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link BoundedWindow#TIMESTAMP_MIN_VALUE}
   */
  @Override
  public Instant getSynchronizedProcessingOutputWatermark() {
    return BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  /**
   * {@inheritDoc}
   *
   * @return a copy of itself always.
   */
  @Override
  public CommittedBundle<Object> withElements(Iterable<WindowedValue<Object>> elements) {
    return this;
  }
}
