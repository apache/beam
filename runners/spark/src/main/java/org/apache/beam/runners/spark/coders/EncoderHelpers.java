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

package org.apache.beam.runners.spark.coders;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import scala.Tuple2;

/**
 * {@link Encoders} utility class.
 */
public class EncoderHelpers {

  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> encoder() {
    return Encoders.kryo((Class<T>) Object.class);
  }

  public static <T1, T2> Encoder<Tuple2<T1, T2>> tuple2Encoder() {
    return Encoders.tuple(EncoderHelpers.<T1>encoder(), EncoderHelpers.<T2>encoder());
  }
}
