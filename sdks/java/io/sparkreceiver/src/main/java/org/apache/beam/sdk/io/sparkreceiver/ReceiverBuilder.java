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
package org.apache.beam.sdk.io.sparkreceiver;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.annotation.Nullable;
import org.apache.commons.lang.ClassUtils;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Class for building an instance for {@link Receiver} that uses Apache Beam mechanisms instead of
 * Spark environment.
 */
@SuppressWarnings({
  "unchecked",
  "argument.type.incompatible",
  "return.type.incompatible",
  "dereference.of.nullable"
})
public class ReceiverBuilder<X, T extends Receiver<X>> implements Serializable {

  private final Class<T> sparkReceiverClass;
  private @Nullable Object[] constructorArgs;

  public ReceiverBuilder(Class<T> sparkReceiverClass) {
    this.sparkReceiverClass = sparkReceiverClass;
  }

  /** Method for specifying constructor arguments for corresponding {@link #sparkReceiverClass}. */
  public ReceiverBuilder<X, T> withConstructorArgs(Object... args) {
    this.constructorArgs = args;
    return this;
  }

  /**
   * @return Proxy for given {@param receiver} that doesn't use Spark environment and uses Apache
   *     Beam mechanisms instead.
   */
  public T build()
      throws InvocationTargetException, InstantiationException, IllegalAccessException {

    if (constructorArgs == null) {
      throw new IllegalStateException(
          "It is not possible to build a Receiver proxy without setting the obligatory parameters.");
    }
    Constructor<?> currentConstructor = null;
    for (Constructor<?> constructor : sparkReceiverClass.getDeclaredConstructors()) {
      Class<?>[] paramTypes = constructor.getParameterTypes();
      if (paramTypes.length != constructorArgs.length) {
        continue;
      }
      boolean matches = true;
      for (int i = 0; i < constructorArgs.length; i++) {
        Object arg = constructorArgs[i];
        if (arg == null) {
          throw new IllegalArgumentException("All args must be not null!");
        }
        Class<?> currArgClass = paramTypes[i];
        if (currArgClass.isPrimitive()) {
          currArgClass = ClassUtils.primitiveToWrapper(currArgClass);
        }
        if (!currArgClass.equals(arg.getClass())) {
          matches = false;
          break;
        }
      }
      if (matches) {
        currentConstructor = constructor;
      }
    }
    if (currentConstructor == null) {
      throw new IllegalStateException("Can not find appropriate constructor!");
    }
    currentConstructor.setAccessible(true);
    return (T) currentConstructor.newInstance(constructorArgs);
  }

  public Class<T> getSparkReceiverClass() {
    return sparkReceiverClass;
  }
}
