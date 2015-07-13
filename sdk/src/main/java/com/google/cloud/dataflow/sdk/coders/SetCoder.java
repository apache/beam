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

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.util.PropertyNames;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@code SetCoder<T>} encodes any {@code Set<T>}
 * as an encoding of an iterable of its elements. The elements
 * may not be in a deterministic order, depending on the
 * {@code Set} implementation.
 *
 * @param <T> the type of the elements of the set
 */
public class SetCoder<T> extends IterableLikeCoder<T, Set<T>> {

  private static final long serialVersionUID = 0L;

  /**
   * Produces a SetCoder with the given elementCoder.
   */
  public static <T> SetCoder<T> of(Coder<T> elementCoder) {
    return new SetCoder<>(elementCoder);
  }

  /**
   * Dynamically typed constructor for JSON deserialization.
   */
  @JsonCreator
  public static SetCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Object> components) {
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
    return of((Coder<?>) components.get(0));
  }

  /**
   * Not all sets have a deterministic encoding.
   *
   * <p> For example, {@code HashSet} comparison does not depend on element order, so
   * two {@code HashSet} instances may be equal but produce different encodings.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Ordering of elements in a set may be non-deterministic.");
  }

  /**
   * Returns the first element in this set if it is non-empty,
   * otherwise returns {@code null}.
   */
  public static <T> List<Object> getInstanceComponents(
      Set<T> exampleValue) {
    return getInstanceComponentsHelper(exampleValue);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  @Override
  protected final Set<T> decodeToIterable(List<T> decodedElements) {
    return new HashSet<>(decodedElements);
  }

  protected SetCoder(Coder<T> elemCoder) {
    super(elemCoder);
  }
}
