/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Dummy serializer for singleton classes.
 *
 * @param <T> instance type
 */
public class SingletonSerializer<T> extends Serializer<T> {

  /**
   * Construct a new singleton serializer.
   *
   * @param methodName name of static method to get singleton instance from the class
   * @param <T> type of class serializer can handle
   * @return serializer
   */
  public static <T> SingletonSerializer<T> of(String methodName) {
    return new SingletonSerializer<>(methodName);
  }

  /** Name of static method to get singleton instance from the class. */
  private final String methodName;

  /** Cached singleton instance. */
  private T instance;

  private SingletonSerializer(String methodName) {
    this.methodName = methodName;
  }

  @Override
  public void write(Kryo kryo, Output output, T object) {}

  @Override
  @SuppressWarnings("unchecked")
  public T read(Kryo kryo, Input input, Class<T> type) {
    try {
      if (instance == null) {
        final Method method = kryo.getClassLoader().loadClass(type.getName()).getMethod(methodName);
        if (!Modifier.isStatic(method.getModifiers())) {
          throw new KryoException(
              "Method " + type.getName() + "#" + methodName + " is not static.");
        }
        instance = (T) method.invoke(null);
      }
      return instance;
    } catch (IllegalAccessException
        | ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new KryoException(e);
    }
  }
}
