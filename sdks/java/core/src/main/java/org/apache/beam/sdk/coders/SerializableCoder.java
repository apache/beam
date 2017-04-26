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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} for Java classes that implement {@link Serializable}.
 *
 * <p>To use, specify the coder type on a PCollection:
 * <pre>
 * {@code
 *   PCollection<MyRecord> records =
 *       foo.apply(...).setCoder(SerializableCoder.of(MyRecord.class));
 * }
 * </pre>
 *
 * <p>{@link SerializableCoder} does not guarantee a deterministic encoding, as Java
 * serialization may produce different binary encodings for two equivalent
 * objects.
 *
 * @param <T> the type of elements handled by this coder
 */
public class SerializableCoder<T extends Serializable> extends CustomCoder<T> {

  /**
   * Returns a {@link SerializableCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T extends Serializable> SerializableCoder<T> of(TypeDescriptor<T> type) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) type.getRawType();
    return new SerializableCoder<>(clazz, type);
  }

  /**
   * Returns a {@link SerializableCoder} instance for the provided element class.
   * @param <T> the element type
   */
  public static <T extends Serializable> SerializableCoder<T> of(Class<T> clazz) {
    return new SerializableCoder<>(clazz, TypeDescriptor.of(clazz));
  }

  /**
   * A {@link CoderProvider} that constructs a {@link SerializableCoder}
   * for any class that implements serializable.
   */
  public static final CoderProvider PROVIDER = new CoderProvider() {
    @Override
    public <T> Coder<T> getCoder(TypeDescriptor<T> typeDescriptor)
        throws CannotProvideCoderException {
      Class<?> clazz = typeDescriptor.getRawType();
      if (Serializable.class.isAssignableFrom(clazz)) {
        @SuppressWarnings("unchecked")
        Class<? extends Serializable> serializableClazz =
            (Class<? extends Serializable>) clazz;
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) SerializableCoder.of(serializableClazz);
        return coder;
      } else {
        throw new CannotProvideCoderException(
            "Cannot provide SerializableCoder because " + typeDescriptor
            + " does not implement Serializable");
      }
    }
  };


  private final Class<T> type;
  private final TypeDescriptor<T> typeDescriptor;

  protected SerializableCoder(Class<T> type, TypeDescriptor<T> typeDescriptor) {
    this.type = type;
    this.typeDescriptor = typeDescriptor;
  }

  public Class<T> getRecordType() {
    return type;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    try {
      ObjectOutputStream oos = new ObjectOutputStream(outStream);
      oos.writeObject(value);
      oos.flush();
    } catch (IOException exn) {
      throw new CoderException("unable to serialize record " + value, exn);
    }
  }

  @Override
  public T decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    try {
      ObjectInputStream ois = new ObjectInputStream(inStream);
      return type.cast(ois.readObject());
    } catch (ClassNotFoundException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. Java serialization is not
   *         deterministic with respect to {@link Object#equals} for all types.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Java Serialization may be non-deterministic.");
  }

  @Override
  public boolean equals(Object other) {
    return !(other == null || getClass() != other.getClass())
            && type == ((SerializableCoder<?>) other).type;
  }

  @Override
  public int hashCode() {
    return type.hashCode();
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return typeDescriptor;
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StructuredCoder. Looks like we cannot do much better
  // in this case.
}
