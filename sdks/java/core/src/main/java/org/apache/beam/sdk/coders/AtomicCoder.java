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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} that has no component {@link Coder Coders} or other configuration.
 *
 * <p>Unless the behavior is overridden, atomic coders are presumed to be deterministic.
 *
 * <p>All atomic coders of the same class are considered to be equal to each other. As a result, an
 * {@link AtomicCoder} should have no associated configuration (instance variables, etc).
 *
 * @param <T> the type of the values being transcoded
 */
public abstract class AtomicCoder<T> extends StructuredCoder<T> {
  /**
   * {@inheritDoc}.
   *
   * <p>Unless overridden, does not throw. An {@link AtomicCoder} is presumed to be deterministic
   *
   * @throws NonDeterministicException if overridden to indicate that this subclass of {@link
   *     AtomicCoder} is not deterministic
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {}

  /**
   * {@inheritDoc}.
   *
   * @return the empty list
   */
  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
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
  public final boolean equals(@Nullable Object other) {
    return other != null && this.getClass().equals(other.getClass());
  }

  /**
   * {@inheritDoc}.
   *
   * @return the {@link #hashCode()} of the {@link Class} of this {@link AtomicCoder}.
   */
  @Override
  public final int hashCode() {
    return this.getClass().hashCode();
  }
}
