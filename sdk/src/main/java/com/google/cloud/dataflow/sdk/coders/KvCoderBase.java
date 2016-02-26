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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A abstract base class for KvCoder. Works around a Jackson2 bug tickled when building
 * {@link KvCoder} directly (as of this writing, Jackson2 walks off the end of
 * an array when it tries to deserialize a class with multiple generic type
 * parameters).  This class should be removed when possible.
 *
 * @param <T> the type of values being transcoded
 */
@Deprecated
public abstract class KvCoderBase<T> extends StandardCoder<T> {
  /**
   * A constructor used only for decoding from JSON.
   *
   * @param typeId present in the JSON encoding, but unused
   * @param isPairLike present in the JSON encoding, but unused
   */
  @Deprecated
  @JsonCreator
  public static KvCoderBase<?> of(
      // N.B. typeId is a required parameter here, since a field named "@type"
      // is presented to the deserializer as an input.
      //
      // If this method did not consume the field, Jackson2 would observe an
      // unconsumed field and a returned value of a derived type.  So Jackson2
      // would attempt to update the returned value with the unconsumed field
      // data.  The standard JsonDeserializer does not implement a mechanism for
      // updating constructed values, so it would throw an exception, causing
      // deserialization to fail.
      @JsonProperty(value = "@type", required = false) String typeId,
      @JsonProperty(value = PropertyNames.IS_PAIR_LIKE, required = false) boolean isPairLike,
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components) {
    return KvCoder.of(components);
  }

  protected KvCoderBase() {}
}
