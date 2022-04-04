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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.apache.commons.lang.ClassUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for building proxy for {@link Receiver} that uses Apache Beam mechanisms instead of Spark
 * environment.
 */
@SuppressWarnings({
  "unchecked",
  "argument.type.incompatible",
  "return.type.incompatible",
  "dereference.of.nullable"
})
public class ProxyReceiverBuilder<X, T extends Receiver<X>> {

  private static final Logger LOG = LoggerFactory.getLogger(ProxyReceiverBuilder.class);
  private final Class<T> sparkReceiverClass;
  private @Nullable Constructor<?> currentConstructor;
  private @Nullable Object[] constructorArgs;
  private @Nullable Consumer<Object[]> storeConsumer;
  private @Nullable T proxy;
  private @Nullable WrappedSupervisor wrappedSupervisor;

  public ProxyReceiverBuilder(Class<T> sparkReceiverClass) {
    this.sparkReceiverClass = sparkReceiverClass;
  }

  /** Method for specifying constructor arguments for corresponding {@link #sparkReceiverClass}. */
  public ProxyReceiverBuilder<X, T> withConstructorArgs(Object... args) {
    for (Constructor<?> constructor : sparkReceiverClass.getDeclaredConstructors()) {
      Class<?>[] paramTypes = constructor.getParameterTypes();
      if (paramTypes.length != args.length) {
        continue;
      }
      boolean matches = true;
      for (int i = 0; i < args.length; i++) {
        Object arg = args[i];
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
        this.constructorArgs = args;
        return this;
      }
    }
    throw new IllegalArgumentException("Can not find appropriate constructor for given args");
  }

  /** Method for specifying custom realization of {@link Receiver#store(Object)} method. */
  public ProxyReceiverBuilder<X, T> withCustomStoreConsumer(Consumer<Object[]> storeConsumer) {
    this.storeConsumer = storeConsumer;
    return this;
  }

  /**
   * @return Proxy for given {@param receiver} that doesn't use Spark environment and uses Apache
   *     Beam mechanisms instead.
   */
  public T build()
      throws InvocationTargetException, InstantiationException, IllegalAccessException {

    if (currentConstructor == null || constructorArgs == null || storeConsumer == null) {
      throw new IllegalStateException(
          "It is not possible to build a Receiver proxy without setting the obligatory parameters.");
    }
    if (proxy != null) {
      throw new IllegalStateException("Proxy already built.");
    }
    currentConstructor.setAccessible(true);
    T originalReceiver = (T) currentConstructor.newInstance(constructorArgs);

    MethodInterceptor handler =
        (obj, method, args, proxy) -> {
          String methodName = method.getName();
          switch (methodName) {
            case "supervisor":
            case "_supervisor":
              return getWrappedSupervisor();
            case "onStart":
              LOG.info("Custom Receiver was started");
              return null;
            case "stop":
              LOG.info("Custom Receiver was stopped. Message = {}", args[0]);
              return null;
            case "store":
              storeConsumer.accept(args);
              return null;
          }
          return proxy.invoke(originalReceiver, args);
        };

    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(sparkReceiverClass);
    enhancer.setCallback(handler);
    this.proxy = (T) enhancer.create(currentConstructor.getParameterTypes(), constructorArgs);
    return this.proxy;
  }

  /**
   * @return {@link org.apache.spark.streaming.receiver.ReceiverSupervisor} that uses Apache Beam
   *     mechanisms.
   */
  private WrappedSupervisor getWrappedSupervisor() {
    if (this.wrappedSupervisor == null) {
      if (this.proxy == null) {
        throw new IllegalStateException(
            "Can not create WrappedSupervisor, because proxy Receiver was not built yet");
      }
      this.wrappedSupervisor = new WrappedSupervisor(this.proxy, new SparkConf());
    }
    return this.wrappedSupervisor;
  }
}
