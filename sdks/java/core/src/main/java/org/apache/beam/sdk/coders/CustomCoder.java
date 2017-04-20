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

import static org.apache.beam.sdk.util.Structs.addString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;

/**
 * An abstract base class for writing a {@link Coder} class that encodes itself via Java
 * serialization.
 *
 * <p>To complete an implementation, subclasses must implement {@link Coder#encode}
 * and {@link Coder#decode} methods. Anonymous subclasses must furthermore override
 * {@link #getEncodingId}.
 *
 * <p>Not to be confused with {@link SerializableCoder} that encodes objects that implement the
 * {@link Serializable} interface.
 *
 * @param <T> the type of elements handled by this coder
 */
public abstract class CustomCoder<T> extends StandardCoder<T>
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
      @JsonProperty(value = "encoding_id", required = false) String encodingId,
      @JsonProperty("type") String type,
      @JsonProperty("serialized_coder") String serializedCoder) {
    return (CustomCoder<?>) SerializableUtils.deserializeFromByteArray(
        StringUtils.jsonStringToByteArray(serializedCoder),
        type);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>Returns an empty list. A {@link CustomCoder} has no default argument {@link Coder coders}.
   */
  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  /**
   * Returns an empty list. A {@link CustomCoder} by default will not have component coders that are
   * used for inference.
   */
  public static <T> List<Object> getInstanceComponents(T exampleValue) {
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * @return A thin {@link CloudObject} wrapping of the Java serialization of {@code this}.
   */
  @Override
  public CloudObject initializeCloudObject() {
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

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException a {@link CustomCoder} is presumed
   * nondeterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "CustomCoder implementations must override verifyDeterministic,"
        + " or they are presumed nondeterministic.");
  }

  /**
   * {@inheritDoc}
   *
   * @return The canonical class name for this coder. For stable data formats that are independent
   *         of class name, it is recommended to override this method.
   *
   * @throws UnsupportedOperationException when an anonymous class is used, since they do not have
   *         a stable canonical class name.
   */
  @Override
  public String getEncodingId() {
    if (getClass().isAnonymousClass()) {
      throw new UnsupportedOperationException(
          String.format("Anonymous CustomCoder subclass %s must override getEncodingId()."
              + " Otherwise, convert to a named class and getEncodingId() will be automatically"
              + " generated from the fully qualified class name.",
              getClass()));
    }
    return getClass().getCanonicalName();
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StandardCoder. Override if we can do better.
}
