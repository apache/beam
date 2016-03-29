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

import static com.google.cloud.dataflow.sdk.util.Structs.addBoolean;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

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

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  @Override
  protected final Iterable<T> decodeToIterable(List<T> decodedElements) {
    return decodedElements;
  }

  @JsonCreator
  public static IterableCoder<?> of(
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
      List<Coder<?>> components) {
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
    return of(components.get(0));
  }

  /**
   * Returns the first element in this iterable if it is non-empty,
   * otherwise returns {@code null}.
   */
  public static <T> List<Object> getInstanceComponents(
      Iterable<T> exampleValue) {
    return getInstanceComponentsHelper(exampleValue);
  }

  protected IterableCoder(Coder<T> elemCoder) {
    super(elemCoder, "Iterable");
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    addBoolean(result, PropertyNames.IS_STREAM_LIKE, true);
    return result;
  }
}
