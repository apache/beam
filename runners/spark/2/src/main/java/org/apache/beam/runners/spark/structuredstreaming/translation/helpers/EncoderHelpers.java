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

import static org.apache.spark.sql.types.DataTypes.BinaryType;

import org.apache.beam.sdk.coders.Coder;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;
import org.checkerframework.checker.nullness.qual.NonNull;

public class EncoderHelpers {
  private static final DataType OBJECT_TYPE = new ObjectType(Object.class);

  /**
   * Wrap a Beam coder into a Spark Encoder using Catalyst Expression Encoders (which uses java code
   * generation).
   */
  public static <T> Encoder<T> fromBeamCoder(Coder<T> coder) {
    Class<? super T> clazz = coder.getEncodedTypeDescriptor().getRawType();
    // Class T could be private, therefore use OBJECT_TYPE to not risk an IllegalAccessError
    return EncoderFactory.create(
        beamSerializer(rootRef(OBJECT_TYPE, true), coder),
        beamDeserializer(rootCol(BinaryType), coder),
        clazz);
  }

  /** Catalyst Expression that serializes elements using Beam {@link Coder}. */
  private static <T> Expression beamSerializer(Expression obj, Coder<T> coder) {
    Expression[] args = {obj, lit(coder, Coder.class)};
    return EncoderFactory.invokeIfNotNull(CoderHelpers.class, "toByteArray", BinaryType, args);
  }

  /** Catalyst Expression that deserializes elements using Beam {@link Coder}. */
  private static <T> Expression beamDeserializer(Expression bytes, Coder<T> coder) {
    Expression[] args = {bytes, lit(coder, Coder.class)};
    return EncoderFactory.invokeIfNotNull(CoderHelpers.class, "fromByteArray", OBJECT_TYPE, args);
  }

  private static Expression rootRef(DataType dt, boolean nullable) {
    return new BoundReference(0, dt, nullable);
  }

  private static Expression rootCol(DataType dt) {
    return new GetColumnByOrdinal(0, dt);
  }

  private static <T extends @NonNull Object> Literal lit(T obj, Class<? extends T> cls) {
    return Literal.fromObject(obj, new ObjectType(cls));
  }
}
