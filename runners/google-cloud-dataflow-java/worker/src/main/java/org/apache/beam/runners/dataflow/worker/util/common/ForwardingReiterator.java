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
package org.apache.beam.runners.dataflow.worker.util.common;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.util.common.Reiterator;

/**
 * A {@link Reiterator} that forwards to another {@code Reiterator}, useful for implementing {@code
 * Reiterator} wrappers.
 *
 * @param <T> the type of elements returned by this iterator
 */
public abstract class ForwardingReiterator<T> implements Reiterator<T>, Cloneable {
  private Reiterator<T> base;

  /**
   * Constructs a {@link ForwardingReiterator}.
   *
   * @param base supplies a base reiterator to forward requests to. This reiterator will be used
   *     directly; it will not be copied by the constructor.
   */
  public ForwardingReiterator(Reiterator<T> base) {
    this.base = checkNotNull(base);
  }

  @Override
  protected ForwardingReiterator<T> clone() {
    ForwardingReiterator<T> result;
    try {
      @SuppressWarnings("unchecked")
      ForwardingReiterator<T> declResult = (ForwardingReiterator<T>) super.clone();
      result = declResult;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError(
          "Object.clone() for a ForwardingReiterator<T> threw "
              + "CloneNotSupportedException; this should not happen, "
              + "since ForwardingReiterator<T> implements Cloneable.",
          e);
    }
    result.base = base.copy();
    return result;
  }

  @Override
  public boolean hasNext() {
    return base.hasNext();
  }

  @Override
  public T next() {
    return base.next();
  }

  @Override
  public void remove() {
    base.remove();
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation uses {@link #clone} to construct a duplicate of the {@link Reiterator}.
   * Derived classes must either implement {@link Cloneable} semantics, or must provide an
   * alternative implementation of this method.
   */
  @Override
  public ForwardingReiterator<T> copy() {
    return clone();
  }
}
