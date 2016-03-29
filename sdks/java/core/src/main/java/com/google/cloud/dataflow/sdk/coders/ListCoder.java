/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.common.base.Preconditions;

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
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
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
