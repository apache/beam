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
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Coder} for Java classes that implement {@link Serializable}.
 *
 * <p>To use, specify the coder type on a PCollection:
 *
 * <pre>{@code
 * PCollection<MyRecord> records =
 *     foo.apply(...).setCoder(SerializableCoder.of(MyRecord.class));
 * }</pre>
 *
 * <p>{@link SerializableCoder} does not guarantee a deterministic encoding, as Java serialization
 * may produce different binary encodings for two equivalent objects.
 *
 * @param <T> the type of elements handled by this coder
 */
public class SerializableCoder<T extends Serializable> extends CustomCoder<T> {

  /*
   * A thread safe set containing classes which we have warned about.
   * Note that we specifically use a weak hash map to allow for classes to be unloaded.
   */
  private static final Set<Class<?>> MISSING_EQUALS_METHOD =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

  private static final Logger LOG = LoggerFactory.getLogger(SerializableCoder.class);

  /**
   * Returns a {@link SerializableCoder} instance for the provided element type.
   *
   * @param <T> the element type
   */
  public static <T extends Serializable> SerializableCoder<T> of(TypeDescriptor<T> type) {
    @SuppressWarnings("unchecked")
    Class<T> clazz = (Class<T>) type.getRawType();
    return new SerializableCoder<>(clazz, type);
  }

  @Override
  public boolean consistentWithEquals() {
    return false;
  }

  /**
   * The structural value of the object is the object itself. The {@link SerializableCoder} should
   * be only used for objects with a proper {@link Object#equals} implementation.
   */
  @Override
  public Object structuralValue(T value) {
    return value;
  }

  /**
   * Returns a {@link SerializableCoder} instance for the provided element class.
   *
   * @param <T> the element type
   */
  public static <T extends Serializable> SerializableCoder<T> of(Class<T> clazz) {
    checkEqualsMethodDefined(clazz);
    return new SerializableCoder<>(clazz, TypeDescriptor.of(clazz));
  }

  private static <T extends Serializable> void checkEqualsMethodDefined(Class<T> clazz) {
    boolean warn = true;
    if (!clazz.isInterface()) {
      Method method;
      try {
        method = clazz.getMethod("equals", Object.class);
      } catch (NoSuchMethodException e) {
        // All concrete classes have an equals method declared in their class hierarchy.
        throw new AssertionError(String.format("Concrete class %s has no equals method", clazz));
      }
      // Check if not default Object#equals implementation.
      warn = Object.class.equals(method.getDeclaringClass());
    }

    // Note that the order of these checks is important since we want the
    // "did we add the class to the set" check to happen last.
    if (warn && MISSING_EQUALS_METHOD.add(clazz)) {
      LOG.warn(
          "Can't verify serialized elements of type {} have well defined equals method. "
              + "This may produce incorrect results on some {}",
          clazz.getSimpleName(),
          PipelineRunner.class.getSimpleName());
    }
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link SerializableCoder} if possible for all
   * types.
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
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (Serializable.class.isAssignableFrom(typeDescriptor.getRawType())) {
        return SerializableCoder.of((TypeDescriptor) typeDescriptor);
      }
      throw new CannotProvideCoderException(
          "Cannot provide SerializableCoder because "
              + typeDescriptor
              + " does not implement Serializable");
    }
  }

  private final Class<T> type;

  /** Access via {@link #getEncodedTypeDescriptor()}. */
  private transient @Nullable TypeDescriptor<T> typeDescriptor;

  protected SerializableCoder(Class<T> type, TypeDescriptor<T> typeDescriptor) {
    this.type = type;
    this.typeDescriptor = typeDescriptor;
  }

  public Class<T> getRecordType() {
    return type;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outStream);
    oos.writeObject(value);
    oos.flush();
  }

  @Override
  public T decode(InputStream inStream) throws IOException, CoderException {
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
   * @throws NonDeterministicException always. Java serialization is not deterministic with respect
   *     to {@link Object#equals} for all types.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "Java Serialization may be non-deterministic.");
  }

  @Override
  public boolean equals(@Nullable Object other) {
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

  @Override
  public String toString() {
    return "SerializableCoder(" + type.getName() + ")";
  }

  // This coder inherits isRegisterByteSizeObserverCheap,
  // getEncodedElementByteSize and registerByteSizeObserver
  // from StructuredCoder. Looks like we cannot do much better
  // in this case.
}
