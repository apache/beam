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

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstract base class to implement a {@link Coder} that defines equality, hashing, and printing
 * via the class name and recursively using {@link #getComponents}.
 *
 * <p>A {@link StructuredCoder} should be defined purely in terms of its component coders, and
 * contain no additional configuration.
 *
 * <p>To extend {@link StructuredCoder}, override the following methods as appropriate:
 *
 * <ul>
 *   <li>{@link #getComponents}: the default implementation returns {@link #getCoderArguments}.
 *   <li>{@link #getEncodedElementByteSize} and {@link #isRegisterByteSizeObserverCheap}: the
 *       default implementation encodes values to bytes and counts the bytes, which is considered
 *       expensive. The default element byte size observer uses the value returned by {@link
 *       #getEncodedElementByteSize}.
 * </ul>
 */
public abstract class StructuredCoder<T> extends Coder<T> {
  protected StructuredCoder() {}

  /**
   * Returns the list of {@link Coder Coders} that are components of this {@link Coder}.
   *
   * <p>The default components will be equal to the value returned by {@link #getCoderArguments()}.
   */
  public List<? extends Coder<?>> getComponents() {
    return getCoderArguments();
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if the two {@link StructuredCoder} instances have the same class and equal
   *     components.
   */
  @Override
  public boolean equals(@Nullable Object o) {
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
}
