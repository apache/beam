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

import java.lang.reflect.Constructor;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.types.DataType;
import scala.collection.immutable.Nil$;
import scala.collection.mutable.WrappedArray;
import scala.reflect.ClassTag;

public class EncoderFactory {
  // default constructor to reflectively create static invoke expressions
  private static final Constructor<StaticInvoke> STATIC_INVOKE_CONSTRUCTOR =
      (Constructor<StaticInvoke>) StaticInvoke.class.getConstructors()[0];

  static <T> ExpressionEncoder<T> create(
      Expression serializer, Expression deserializer, Class<? super T> clazz) {
    return new ExpressionEncoder<>(serializer, deserializer, ClassTag.apply(clazz));
  }

  /**
   * Invoke method {@code fun} on Class {@code cls}, immediately propagating {@code null} if any
   * input arg is {@code null}.
   *
   * <p>To address breaking interfaces between various version of Spark 3 these are created
   * reflectively. This is fine as it's just needed once to create the query plan.
   */
  static Expression invokeIfNotNull(Class<?> cls, String fun, DataType type, Expression... args) {
    try {
      switch (STATIC_INVOKE_CONSTRUCTOR.getParameterCount()) {
        case 6:
          // Spark 3.1.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, new WrappedArray.ofRef<>(args), true, true);
        case 8:
          // Spark 3.2.x, 3.3.x
          return STATIC_INVOKE_CONSTRUCTOR.newInstance(
              cls, type, fun, new WrappedArray.ofRef<>(args), Nil$.MODULE$, true, true, true);
        default:
          throw new RuntimeException("Unsupported version of Spark");
      }
    } catch (IllegalArgumentException | ReflectiveOperationException ex) {
      throw new RuntimeException(ex);
    }
  }
}
