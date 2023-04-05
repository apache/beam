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
 * A {@link Coder} for {@link List}, using the format of {@link IterableLikeCoder}.
 *
 * @param <T> the type of the elements of the Lists being transcoded
 */
public class ListCoder<T> extends IterableLikeCoder<T, List<T>> {

  public static <T> ListCoder<T> of(Coder<T> elemCoder) {
    return new ListCoder<>(elemCoder);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  @Override
  protected final List<T> decodeToIterable(List<T> decodedElements) {
    return decodedElements;
  }

  protected ListCoder(Coder<T> elemCoder) {
    super(elemCoder, "List");
  }

  @Override
  public boolean consistentWithEquals() {
    return getElemCoder().consistentWithEquals();
  }

  @Override
  public Object structuralValue(List<T> values) {
    if (consistentWithEquals()) {
      return values;
    } else {
      List<Object> ret = new ArrayList<>(values.size());
      for (T value : values) {
        ret.add(getElemCoder().structuralValue(value));
      }
      return ret;
    }
  }

  /**
   * List sizes are always known, so ListIterable may be deterministic while the general
   * IterableLikeCoder is not.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "ListCoder.elemCoder must be deterministic", getElemCoder());
  }

  @Override
  public TypeDescriptor<List<T>> getEncodedTypeDescriptor() {
    return new TypeDescriptor<List<T>>(getClass()) {}.where(
        new TypeParameter<T>() {}, getElemCoder().getEncodedTypeDescriptor());
  }
}
