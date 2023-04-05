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
package org.apache.beam.sdk.io.hadoop;

import com.google.auto.service.AutoService;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviderRegistrar;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code WritableCoder} is a {@link Coder} for a Java class that implements {@link Writable}.
 *
 * <p>To use, specify the coder type on a PCollection:
 *
 * <pre>{@code
 * PCollection<MyRecord> records =
 *     foo.apply(...).setCoder(WritableCoder.of(MyRecord.class));
 * }</pre>
 *
 * @param <T> the type of elements handled by this coder.
 */
public class WritableCoder<T extends Writable> extends CustomCoder<T> {
  private static final long serialVersionUID = 0L;

  /**
   * Returns a {@code WritableCoder} instance for the provided element class.
   *
   * @param <T> the element type
   */
  public static <T extends Writable> WritableCoder<T> of(Class<T> clazz) {
    return new WritableCoder<>(clazz);
  }

  private final Class<T> type;

  @SuppressWarnings("WeakerAccess")
  public WritableCoder(Class<T> type) {
    this.type = type;
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    value.write(new DataOutputStream(outStream));
  }

  @SuppressWarnings("unchecked")
  @Override
  public T decode(InputStream inStream) throws IOException {
    try {
      if (type == NullWritable.class) {
        // NullWritable has no default constructor
        return (T) NullWritable.get();
      }
      T t = type.getDeclaredConstructor().newInstance();
      t.readFields(new DataInputStream(inStream));
      return t;
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  @Override
  public List<Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "Hadoop Writable may be non-deterministic.");
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof WritableCoder)) {
      return false;
    }
    WritableCoder<?> that = (WritableCoder<?>) other;
    return Objects.equals(this.type, that.type);
  }

  @Override
  public int hashCode() {
    return type.hashCode();
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link WritableCoder} for Hadoop {@link Writable
   * writable types}.
   *
   * <p>This method is invoked reflectively from {@link DefaultCoder}.
   */
  @SuppressWarnings("WeakerAccess")
  public static CoderProvider getCoderProvider() {
    return new WritableCoderProvider();
  }

  /**
   * A {@link CoderProviderRegistrar} which registers a {@link CoderProvider} which can handle
   * {@link Writable writable types}.
   */
  @AutoService(CoderProviderRegistrar.class)
  public static class WritableCoderProviderRegistrar implements CoderProviderRegistrar {

    @Override
    public List<CoderProvider> getCoderProviders() {
      return Collections.singletonList(getCoderProvider());
    }
  }

  /** A {@link CoderProvider} for Hadoop {@link Writable writable types}. */
  private static class WritableCoderProvider extends CoderProvider {
    private static final TypeDescriptor<Writable> WRITABLE_TYPE = new TypeDescriptor<Writable>() {};

    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(WRITABLE_TYPE)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s does not implement the interface %s",
                WritableCoder.class.getSimpleName(), typeDescriptor, Writable.class.getName()));
      }

      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = WritableCoder.of((Class) typeDescriptor.getRawType());
        return coder;
      } catch (IllegalArgumentException e) {
        throw new CannotProvideCoderException(e);
      }
    }
  }
}
