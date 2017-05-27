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

package org.apache.beam.sdk.coders;

import java.util.Collections;
import java.util.List;

/**
 * A {@link Coder} that has no component {@link Coder Coders} or other state.
 *
 * <p>Note that, unless the behavior is overridden, atomic coders are presumed to be deterministic.
 *
 * <p>All atomic coders of the same class are considered to be equal to each other. As a result,
 * an {@link AtomicCoder} should have no associated state.
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class AtomicCoder<T> extends StructuredCoder<T> {
  /**
   * {@inheritDoc}.
   *
   * @throws NonDeterministicException
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {}

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  /**
   * {@inheritDoc}.
   *
   * @return the empty {@link List}.
   */
  @Override
  public final List<? extends Coder<?>> getComponents() {
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}.
   *
   * @return true if the other object has the same class as this {@link AtomicCoder}.
   */
  @Override
  public final boolean equals(Object other) {
    return other != null && this.getClass().equals(other.getClass());
  }

  @Override
  public final int hashCode() {
    return this.getClass().hashCode();
  }
}
