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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Class for building an instance for {@link Receiver} that uses Apache Beam mechanisms instead of
 * Spark environment.
 */
public class ReceiverBuilder<X, T extends Receiver<X>> implements Serializable {

  private final Class<T> sparkReceiverClass;
  private Object[] constructorArgs;

  public ReceiverBuilder(Class<T> sparkReceiverClass) {
    this.sparkReceiverClass = sparkReceiverClass;
    this.constructorArgs = new Object[0];
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

    Constructor<?> currentConstructor = null;
    for (Constructor<?> constructor : sparkReceiverClass.getDeclaredConstructors()) {
      Class<?>[] paramTypes = constructor.getParameterTypes();
      if (paramTypes.length != constructorArgs.length) {
        continue;
      }
      boolean matches = true;
      for (int i = 0; i < constructorArgs.length; i++) {
        Object arg = constructorArgs[i];

        checkArgument(arg != null, "All args must be not null!");

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

    checkStateNotNull(currentConstructor, "Can not find appropriate constructor!");

    currentConstructor.setAccessible(true);
    return sparkReceiverClass.cast(currentConstructor.newInstance(constructorArgs));
  }

  public Class<T> getSparkReceiverClass() {
    return sparkReceiverClass;
  }
}
