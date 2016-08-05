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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.util.PropertyNames;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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

  @JsonCreator
  public static ListCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    checkArgument(components.size() == 1, "Expecting 1 component, got %s", components.size());
    return of((Coder<?>) components.get(0));
  }

  /**
   * Returns the first element in this list if it is non-empty,
   * otherwise returns {@code null}.
   */
  public static <T> List<Object> getInstanceComponents(List<T> exampleValue) {
    return getInstanceComponentsHelper(exampleValue);
  }

  protected ListCoder(Coder<T> elemCoder) {
    super(elemCoder, "List");
  }

  /**
   * List sizes are always known, so ListIterable may be deterministic while
   * the general IterableLikeCoder is not.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(
        "ListCoder.elemCoder must be deterministic", getElemCoder());
  }

}
