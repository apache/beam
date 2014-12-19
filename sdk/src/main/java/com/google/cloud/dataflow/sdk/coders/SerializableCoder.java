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

import com.google.cloud.dataflow.sdk.util.CloudObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * An encoder of {@link java.io.Serializable} objects.
 *
 * To use, specify the coder type on a PCollection.
 * <pre>
 * {@code
 *   PCollection<MyRecord> records =
 *       foo.apply(...).setCoder(SerializableCoder.of(MyRecord.class));
 * }
 * </pre>
 *
 * <p> SerializableCoder does not guarantee a deterministic encoding, as Java
 * Serialization may produce different binary encodings for two equivalent
 * objects.
 *
 * @param <T> the type of elements handled by this coder
 */
@SuppressWarnings("serial")
public class SerializableCoder<T extends Serializable>
    extends AtomicCoder<T> {
  /**
   * Returns a {@code SerializableCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T extends Serializable> SerializableCoder<T> of(Class<T> type) {
    return new SerializableCoder<>(type);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static SerializableCoder<?> of(@JsonProperty("type") String classType)
      throws ClassNotFoundException {
    Class<?> clazz = Class.forName(classType);
    if (!Serializable.class.isAssignableFrom(clazz)) {
      throw new ClassNotFoundException(
          "Class " + classType + " does not implement Serializable");
    }
    return of((Class<? extends Serializable>) clazz);
  }

  private final Class<T> type;

  protected SerializableCoder(Class<T> type) {
    this.type = type;
  }

  public Class<T> getRecordType() {
    return type;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null record");
    }
    try (ObjectOutputStream oos = new ObjectOutputStream(outStream)) {
      oos.writeObject(value);
    } catch (IOException exn) {
      throw new CoderException("unable to serialize record " + value, exn);
    }
  }

  @Override
  public T decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    try (ObjectInputStream ois = new ObjectInputStream(inStream)) {
      return type.cast(ois.readObject());
    } catch (ClassNotFoundException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    result.put("type", type.getName());
    return result;
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public boolean equals(Object other) {
    if (getClass() != other.getClass()) {
      return false;
    }
    return type == ((SerializableCoder) other).type;
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StandardCoder. Looks like we cannot do much better
  // in this case.
}
