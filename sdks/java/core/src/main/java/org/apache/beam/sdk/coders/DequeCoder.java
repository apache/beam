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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * A {@link Coder} for {@link Deque}, using the format of {@link IterableLikeCoder}.
 *
 * @param <T> the type of the elements of the Deques being transcoded
 */
public class DequeCoder<T> extends IterableLikeCoder<T, Deque<T>> {

  public static <T> DequeCoder<T> of(Coder<T> elemCoder) {
    return new DequeCoder<>(elemCoder);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  @Override
  protected Deque<T> decodeToIterable(List<T> decodedElements) {
    return new ArrayDeque<>(decodedElements);
  }

  protected DequeCoder(Coder<T> elemCoder) {
    super(elemCoder, "Deque");
  }

  @Override
  public boolean consistentWithEquals() {
    return getElemCoder().consistentWithEquals();
  }

  @Override
  public Object structuralValue(Deque<T> values) {
    if (consistentWithEquals()) {
      return values;
    } else {
      final Deque<Object> ret = new ArrayDeque<>(values.size());
      for (T value : values) {
        ret.add(getElemCoder().structuralValue(value));
      }
      return ret;
    }
  }

  /**
   * Deque sizes are always known, so DequeIterable may be deterministic while the general
   * IterableLikeCoder is not.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(
        this, "Coder for elements of DequeCoder must be determistic", getElemCoder());
  }

  @Override
  public TypeDescriptor<Deque<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Deque<T>>(getClass()) {}.where(
        new TypeParameter<T>() {}, getElemCoder().getEncodedTypeDescriptor());
  }
}
