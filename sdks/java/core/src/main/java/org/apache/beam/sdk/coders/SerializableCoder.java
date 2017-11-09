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

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
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
   * Returns a {@link CoderProvider} which uses the {@link SerializableCoder} if possible for
   * all types.
   *
   * <p>This method is invoked reflectively from {@link DefaultCoder}.
   */
  @SuppressWarnings("unused")
  public static CoderProvider getCoderProvider() {
    return new SerializableCoderProvider();
  }

  /**
   * A {@link CoderProviderRegistrar} which registers a {@link CoderProvider} which can handle
   * serializable types.
   */
  public static class SerializableCoderProviderRegistrar implements CoderProviderRegistrar {

    @Override
    public List<CoderProvider> getCoderProviders() {
      return ImmutableList.of(getCoderProvider());
    }
  }

  /**
   * A {@link CoderProvider} that constructs a {@link SerializableCoder} for any class that
   * implements serializable.
   */
  static class SerializableCoderProvider extends CoderProvider {
    @Override
    public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor,
        List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
      if (Serializable.class.isAssignableFrom(typeDescriptor.getRawType())) {
        return SerializableCoder.of((TypeDescriptor) typeDescriptor);
      }
      throw new CannotProvideCoderException(
          "Cannot provide SerializableCoder because " + typeDescriptor
              + " does not implement Serializable");
    }
  }

  private final Class<T> type;

  /** Access via {@link #getEncodedTypeDescriptor()}. */
  @Nullable private transient TypeDescriptor<T> typeDescriptor;

  protected SerializableCoder(Class<T> type, TypeDescriptor<T> typeDescriptor) {
    this.type = type;
    this.typeDescriptor = typeDescriptor;
  }

  public Class<T> getRecordType() {
    return type;
  }

  @Override
  public void encode(T value, OutputStream outStream)
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
  public T decode(InputStream inStream)
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
    if (typeDescriptor == null) {
      typeDescriptor = TypeDescriptor.of(type);
    }
    return typeDescriptor;
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StructuredCoder. Looks like we cannot do much better
  // in this case.
}
