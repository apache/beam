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

package org.apache.beam.runners.spark.coders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.util.CloudObject;
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
 * @param <T> the type of elements handled by this coder
 */
public class WritableCoder<T extends Writable> extends StandardCoder<T> {
  private static final long serialVersionUID = 0L;

  /**
   * Returns a {@code WritableCoder} instance for the provided element class.
   * @param <T> the element type
   * @param clazz the element class
   * @return a {@code WritableCoder} instance for the provided element class
   */
  public static <T extends Writable> WritableCoder<T> of(Class<T> clazz) {
    if (clazz.equals(NullWritable.class)) {
      @SuppressWarnings("unchecked")
      WritableCoder<T> result = (WritableCoder<T>) NullWritableCoder.of();
      return result;
    }
    return new WritableCoder<>(clazz);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static WritableCoder<?> of(@JsonProperty("type") String classType)
      throws ClassNotFoundException {
    Class<?> clazz = Class.forName(classType);
    if (!Writable.class.isAssignableFrom(clazz)) {
      throw new ClassNotFoundException(
          "Class " + classType + " does not implement Writable");
    }
    return of((Class<? extends Writable>) clazz);
  }

  private final Class<T> type;

  public WritableCoder(Class<T> type) {
    this.type = type;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context) throws IOException {
    value.write(new DataOutputStream(outStream));
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    try {
      T t = type.getConstructor().newInstance();
      t.readFields(new DataInputStream(inStream));
      return t;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new CoderException("unable to deserialize record", e);
    } catch (InvocationTargetException ite) {
      throw new CoderException("unable to deserialize record", ite.getCause());
    }
  }

  @Override
  public List<Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  protected CloudObject initializeCloudObject() {
    CloudObject result = CloudObject.forClass(getClass());
    result.put("type", type.getName());
    return result;
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {
    throw new NonDeterministicException(this,
        "Hadoop Writable may be non-deterministic.");
  }

}
