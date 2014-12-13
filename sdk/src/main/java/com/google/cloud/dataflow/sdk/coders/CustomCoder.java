/*
 * Copyright (C) 2014 Google Inc.
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

import static com.google.cloud.dataflow.sdk.util.Structs.addString;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * An abstract base class for writing Coders that encodes itself via java
 * serialization.  Subclasses only need to implement the {@link Coder#encode}
 * and {@link Coder#decode} methods.
 *
 * <p>
 * Not to be confused with {@link SerializableCoder} that encodes serializables.
 *
 * @param <T> the type of elements handled by this coder
 */
public abstract class CustomCoder<T> extends AtomicCoder<T>
    implements Serializable {

  @JsonCreator
  public static CustomCoder<?> of(
      // N.B. typeId is a required parameter here, since a field named "@type"
      // is presented to the deserializer as an input.
      //
      // If this method did not consume the field, Jackson2 would observe an
      // unconsumed field and a returned value of a derived type.  So Jackson2
      // would attempt to update the returned value with the unconsumed field
      // data, The standard JsonDeserializer does not implement a mechanism for
      // updating constructed values, so it would throw an exception, causing
      // deserialization to fail.
      @JsonProperty(value = "@type", required = false) String typeId,
      @JsonProperty("type") String type,
      @JsonProperty("serialized_coder") String serializedCoder) {
    return (CustomCoder<?>) SerializableUtils.deserializeFromByteArray(
        StringUtils.jsonStringToByteArray(serializedCoder),
        type);
  }

  @Override
  public CloudObject asCloudObject() {
    // N.B. We use the CustomCoder class, not the derived class, since during
    // deserialization we will be using the CustomCoder's static factory method
    // to construct an instance of the derived class.
    CloudObject result = CloudObject.forClass(CustomCoder.class);
    addString(result, "type", getClass().getName());
    addString(result, "serialized_coder",
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(this)));
    return result;
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StandardCoder. Override if we can do better.
}
