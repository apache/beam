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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.ObjectType;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

public class EncoderFactory {

  public static <T> Encoder<T> fromBeamCoder(Coder<T> coder) {
    Class<? super T> clazz = coder.getEncodedTypeDescriptor().getRawType();
    ClassTag<T> classTag = ClassTag$.MODULE$.apply(clazz);
    Expression serializer =
        new EncoderHelpers.EncodeUsingBeamCoder<>(
            new BoundReference(0, new ObjectType(clazz), true), coder);
    Expression deserializer =
        new EncoderHelpers.DecodeUsingBeamCoder<>(
            new Cast(
                new GetColumnByOrdinal(0, BinaryType), BinaryType, scala.Option.<String>empty()),
            classTag,
            coder);
    return new ExpressionEncoder<>(serializer, deserializer, classTag);
  }
}
