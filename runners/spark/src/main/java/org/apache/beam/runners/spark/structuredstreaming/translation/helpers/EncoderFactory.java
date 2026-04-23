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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.emptyList;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;

import java.lang.reflect.Constructor;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.Invoke;
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.types.DataType;
import scala.Option;
import scala.reflect.ClassTag;

public class EncoderFactory {
  // Resolve the Scala case-class primary constructor (the one with the most parameters).
  // Constructor ordering returned by Class.getConstructors() is JVM-defined and not stable
  // across Spark versions, so we pick the widest constructor explicitly and then dispatch on
  // parameter count below to pick the right argument shape per Spark version.
  private static final Constructor<StaticInvoke> STATIC_INVOKE_CONSTRUCTOR =
      primaryConstructor(StaticInvoke.class);

  private static final Constructor<Invoke> INVOKE_CONSTRUCTOR = primaryConstructor(Invoke.class);

  private static final Constructor<NewInstance> NEW_INSTANCE_CONSTRUCTOR =
      primaryConstructor(NewInstance.class);

  @SuppressWarnings("unchecked")
  private static <T> Constructor<T> primaryConstructor(Class<T> cls) {
    Constructor<?>[] ctors = cls.getConstructors();
    Constructor<?> widest = ctors[0];
    for (int i = 1; i < ctors.length; i++) {
      if (ctors[i].getParameterCount() > widest.getParameterCount()) {
        widest = ctors[i];
      }
    }
    return (Constructor<T>) widest;
  }

  static <T> ExpressionEncoder<T> create(
      Expression serializer, Expression deserializer, Class<? super T> clazz) {
    return new ExpressionEncoder<>(serializer, deserializer, ClassTag.apply(clazz));
  }

  /**
   * Invoke method {@code fun} on Class {@code cls}, immediately propagating {@code null} if any
   * input arg is {@code null}.
   */
  static Expression invokeIfNotNull(Class<?> cls, String fun, DataType type, Expression... args) {
    return invoke(cls, fun, type, true, args);
  }

  /** Invoke method {@code fun} on Class {@code cls}. */
  static Expression invoke(Class<?> cls, String fun, DataType type, Expression... args) {
    return invoke(cls, fun, type, false, args);
  }

  private static Expression invoke(
      Class<?> cls, String fun, DataType type, boolean propagateNull, Expression... args) {
    try {
      // To address breaking interfaces between various version of Spark 3,  expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (STATIC_INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), propagateNull, true);
        case 7:
          // Spark 3.2.0
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), emptyList(), propagateNull, true);
        case 8:
          // Spark 3.2.x, 3.3.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, seqOf(args), emptyList(), propagateNull, true, true);
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Invoke method {@code fun} on {@code obj} with provided {@code args}. */
  static Expression invoke(
      Expression obj, String fun, DataType type, boolean nullable, Expression... args) {
    try {
      // To address breaking interfaces between various version of Spark 3,  expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (STATIC_INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return INVOKE_CONSTRUCTOR.newInstance(obj, fun, type, seqOf(args), false, nullable);
        case 7:
          // Spark 3.2.0
          return INVOKE_CONSTRUCTOR.newInstance(
              obj, fun, type, seqOf(args), emptyList(), false, nullable);
        case 8:
          // Spark 3.2.x, 3.3.x
          return INVOKE_CONSTRUCTOR.newInstance(
              obj, fun, type, seqOf(args), emptyList(), false, nullable, true);
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }

  static Expression newInstance(Class<?> cls, DataType type, Expression... args) {
    try {
      // To address breaking interfaces between various version of Spark 3,  expressions are
      // created reflectively. This is fine as it's just needed once to create the query plan.
      switch (NEW_INSTANCE_CONSTRUCTOR.getParameterCount()) {
        case 5:
          return NEW_INSTANCE_CONSTRUCTOR.newInstance(cls, seqOf(args), true, type, Option.empty());
        case 6:
          return NEW_INSTANCE_CONSTRUCTOR.newInstance(
              cls, seqOf(args), emptyList(), true, type, Option.empty());
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }
}
