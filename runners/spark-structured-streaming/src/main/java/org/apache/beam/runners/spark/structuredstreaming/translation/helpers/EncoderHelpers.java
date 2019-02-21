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

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

/** {@link Encoders} utility class. */
public class EncoderHelpers {

  // 1. use actual classes and not object avoid Spark fallback to GenericRowWithSchema.
  // 2. use raw objects because only raw classes can be used with kryo. Cast to Class<T> to allow
  // the type inference mechanism to infer Encoder<WindowedValue<T>> to get back the type checking

  /*
    --------- Encoders for internal spark runner objects
   */

  /**
   * Get a bytes {@link Encoder} for {@link WindowedValue}. Bytes serialisation is issued by Kryo
   */
  @SuppressWarnings("unchecked") public static <T> Encoder<T> windowedValueEncoder() {
    return Encoders.kryo((Class<T>) WindowedValue.class);
  }

  /** Get a bytes {@link Encoder} for {@link KV}. Bytes serialisation is issued by Kryo */
  @SuppressWarnings("unchecked") public static <T> Encoder<T> kvEncoder() {
    return Encoders.kryo((Class<T>) KV.class);
  }

  /** Get a bytes {@link Encoder} for {@code T}. Bytes serialisation is issued by Kryo */
  @SuppressWarnings("unchecked") public static <T> Encoder<T> genericEncoder() {
    return Encoders.kryo((Class<T>) Object.class);
  }

  /** Get a bytes {@link Encoder} for {@link Tuple2}. Bytes serialisation is issued by Kryo */
  public static <T1, T2> Encoder<Tuple2<T1, T2>> tuple2Encoder() {
    return Encoders.tuple(EncoderHelpers.genericEncoder(), EncoderHelpers.genericEncoder());
  }

  /*
    --------- Bridges from Beam Coders to Spark Encoders
   */

}