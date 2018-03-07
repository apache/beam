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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * An {@link IterableCoder} encodes any {@link Iterable} in the format
 * of {@link IterableLikeCoder}.
 *
 * @param <T> the type of the elements of the iterables being transcoded
 */
public class IterableCoder<T> extends IterableLikeCoder<T, Iterable<T>> {

  public static <T> IterableCoder<T> of(Coder<T> elemCoder) {
    return new IterableCoder<>(elemCoder);
  }

  @Override
  public Object structuralValue(Iterable<T> value) {
    ArrayList<Object> result = new ArrayList<>();
    for (T elem : value) {
      result.add(getElemCoder().structuralValue(elem));
    }
    return result;
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  @Override
  protected final Iterable<T> decodeToIterable(List<T> decodedElements) {
    return decodedElements;
  }

  protected IterableCoder(Coder<T> elemCoder) {
    super(elemCoder, "Iterable");
  }

  @Override
  public TypeDescriptor<Iterable<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<Iterable<T>>() {}.where(
        new TypeParameter<T>() {}, getElemCoder().getEncodedTypeDescriptor());
  }
}
