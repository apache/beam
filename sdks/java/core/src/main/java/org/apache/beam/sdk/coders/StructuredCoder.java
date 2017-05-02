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

import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An abstract base class to implement a {@link Coder} that defines equality, hashing, and printing
 * via the class name and recursively using {@link #getComponents}.
 *
 * <p>To extend {@link StructuredCoder}, override the following methods as appropriate:
 *
 * <ul>
 *   <li>{@link #getComponents}: the default implementation returns {@link #getCoderArguments}.</li>
 *   <li>{@link #getEncodedElementByteSize} and
 *       {@link #isRegisterByteSizeObserverCheap}: the
 *       default implementation encodes values to bytes and counts the bytes, which is considered
 *       expensive.</li>
 * </ul>
 */
public abstract class StructuredCoder<T> implements Coder<T> {
  protected StructuredCoder() {}

  /**
   * Returns the list of {@link Coder Coders} that are components of this {@link Coder}.
   */
  public List<? extends Coder<?>> getComponents() {
    List<? extends Coder<?>> coderArguments = getCoderArguments();
    if (coderArguments == null) {
      return Collections.emptyList();
    } else {
      return coderArguments;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if the two {@link StructuredCoder} instances have the
   * same class and equal components.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    StructuredCoder<?> that = (StructuredCoder<?>) o;
    return this.getComponents().equals(that.getComponents());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode() * 31 + getComponents().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    String s = getClass().getName();
    builder.append(s.substring(s.lastIndexOf('.') + 1));

    List<? extends Coder<?>> componentCoders = getComponents();
    if (!componentCoders.isEmpty()) {
      builder.append('(');
      boolean first = true;
      for (Coder<?> componentCoder : componentCoders) {
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        builder.append(componentCoder.toString());
      }
      builder.append(')');
    }
    return builder.toString();
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code false} unless it is overridden. {@link StructuredCoder#registerByteSizeObserver}
   *         invokes {@link #getEncodedElementByteSize} which requires re-encoding an element
   *         unless it is overridden. This is considered expensive.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(T value, Context context) {
    return false;
  }

  /**
   * Returns the size in bytes of the encoded value using this coder.
   */
  protected long getEncodedElementByteSize(T value, Context context)
      throws Exception {
    try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
      encode(value, os, context);
      return os.getCount();
    } catch (Exception exn) {
      throw new IllegalArgumentException(
          "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>For {@link StructuredCoder} subclasses, this notifies {@code observer} about the byte size
   * of the encoded value using this coder as returned by {@link #getEncodedElementByteSize}.
   */
  @Override
  public void registerByteSizeObserver(
      T value, ElementByteSizeObserver observer, Context context)
      throws Exception {
    observer.update(getEncodedElementByteSize(value, context));
  }

  protected void verifyDeterministic(String message, Iterable<Coder<?>> coders)
      throws NonDeterministicException {
    for (Coder<?> coder : coders) {
      try {
        coder.verifyDeterministic();
      } catch (NonDeterministicException e) {
        throw new NonDeterministicException(this, message, e);
      }
    }
  }

  protected void verifyDeterministic(String message, Coder<?>... coders)
      throws NonDeterministicException {
    verifyDeterministic(message, Arrays.asList(coders));
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code false} for {@link StructuredCoder} unless overridden.
   */
  @Override
  public boolean consistentWithEquals() {
    return false;
  }

  @Override
  public Object structuralValue(T value) {
    if (value != null && consistentWithEquals()) {
      return value;
    } else {
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(value, os, Context.OUTER);
        return new StructuralByteArray(os.toByteArray());
      } catch (Exception exn) {
        throw new IllegalArgumentException(
            "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return (TypeDescriptor<T>)
        TypeDescriptor.of(getClass()).resolveType(new TypeDescriptor<T>() {}.getType());
  }
}
