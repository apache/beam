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

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.listOf;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.seqOf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.types.DataType;
import scala.reflect.ClassTag$;

public class EncoderFactory {

  static <T> Encoder<T> create(
      Expression serializer, Expression deserializer, Class<? super T> clazz) {
    return new ExpressionEncoder<>(
        SchemaHelpers.binarySchema(),
        false,
        listOf(serializer),
        deserializer,
        ClassTag$.MODULE$.apply(clazz));
  }

  /**
   * Invoke method {@code fun} on Class {@code cls}, immediately propagating {@code null} if any
   * input arg is {@code null}.
   */
  static Expression invokeIfNotNull(Class<?> cls, String fun, DataType type, Expression... args) {
    return new StaticInvoke(cls, type, fun, seqOf(args), true, true);
  }
}
