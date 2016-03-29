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

import java.util.Collection;
import java.util.List;

/**
 * A {@link CollectionCoder} encodes {@link Collection Collections} in the format
 * of {@link IterableLikeCoder}.
 */
public class CollectionCoder<T> extends IterableLikeCoder<T, Collection<T>> {

  public static <T> CollectionCoder<T> of(Coder<T> elemCoder) {
    return new CollectionCoder<>(elemCoder);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  /**
   * {@inheritDoc}
   *
   * @return the decoded elements directly, since {@link List} is a subtype of
   * {@link Collection}.
   */
  @Override
  protected final Collection<T> decodeToIterable(List<T> decodedElements) {
    return decodedElements;
  }

  @JsonCreator
  public static CollectionCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Object> components) {
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
    return of((Coder<?>) components.get(0));
  }

  /**
   * Returns the first element in this collection if it is non-empty,
   * otherwise returns {@code null}.
   */
  public static <T> List<Object> getInstanceComponents(
      Collection<T> exampleValue) {
    return getInstanceComponentsHelper(exampleValue);
  }

  protected CollectionCoder(Coder<T> elemCoder) {
    super(elemCoder, "Collection");
  }
}
