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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderFactory;
import org.apache.beam.sdk.coders.CoderFactoryRegistrar;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

/**
 * A {@code WritableCoder} is a {@link Coder} for a Java class that implements {@link Writable}.
 *
 * <p>To use, specify the coder type on a PCollection:
 * <pre>
 * {@code
 *   PCollection<MyRecord> records =
 *       foo.apply(...).setCoder(WritableCoder.of(MyRecord.class));
 * }
 * </pre>
 *
 * @param <T> the type of elements handled by this coder.
 */
public class WritableCoder<T extends Writable> extends CustomCoder<T> {
  private static final long serialVersionUID = 0L;

  /**
   * Returns a {@code WritableCoder} instance for the provided element class.
   * @param <T> the element type
   */
  public static <T extends Writable> WritableCoder<T> of(Class<T> clazz) {
    return new WritableCoder<>(clazz);
  }

  private final Class<T> type;

  public WritableCoder(Class<T> type) {
    this.type = type;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context) throws IOException {
    value.write(new DataOutputStream(outStream));
  }

  @SuppressWarnings("unchecked")
  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    try {
      if (type == NullWritable.class) {
        // NullWritable has no default constructor
        return (T) NullWritable.get();
      }
      T t = type.newInstance();
      t.readFields(new DataInputStream(inStream));
      return t;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  @Override
  public List<Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Hadoop Writable may be non-deterministic.");
  }

  /**
   * Returns a {@link CoderFactory} which uses the {@link WritableCoder} for Hadoop
   * {@link Writable writable types}.
   *
   * <p>This method is invoked reflectively from {@link DefaultCoder}.
   */
  public static CoderFactory getCoderFactory() {
    return new WritableCoderFactory();
  }

  /**
   * A {@link CoderFactoryRegistrar} which registers a {@link CoderFactory} which can handle
   * {@link Writable writable types}.
   */
  @AutoService(CoderFactoryRegistrar.class)
  public static class WritableCoderFactoryRegistrar implements CoderFactoryRegistrar {

    @Override
    public List<CoderFactory> getCoderFactories() {
      return Collections.singletonList(getCoderFactory());
    }
  }

  /**
   * A {@link CoderFactory} for Hadoop {@link Writable writable types}.
   */
  private static class WritableCoderFactory implements CoderFactory {
    private static final TypeDescriptor<Writable> WRITABLE_TYPE = new TypeDescriptor<Writable>() {};

    @Override
    public <T> Coder<T> create(TypeDescriptor<T> typeDescriptor,
        List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(WRITABLE_TYPE)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s does not implement the interface %s",
                WritableCoder.class.getSimpleName(),
                typeDescriptor,
                Writable.class.getName()));
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
