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

package org.apache.beam.sdk.extensions.sql.impl.utils;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Provides converters from {@link BigDecimal} to other numeric types based on
 * the input {@link FieldType}.
 */
public class BigDecimalConverter {

  private static final Map<FieldType, SerializableFunction<BigDecimal, ? extends Number>>
      CONVERTER_MAP = ImmutableMap
      .<FieldType, SerializableFunction<BigDecimal, ? extends Number>>builder()
      .put(FieldType.INT32, BigDecimal::intValue)
      .put(FieldType.INT16, BigDecimal::shortValue)
      .put(FieldType.BYTE, BigDecimal::byteValue)
      .put(FieldType.INT64, BigDecimal::longValue)
      .put(FieldType.FLOAT, BigDecimal::floatValue)
      .put(FieldType.DOUBLE, BigDecimal::doubleValue)
      .put(FieldType.DECIMAL, v -> v)
      .build();

  public static SerializableFunction<BigDecimal, ? extends Number> forSqlType(
      FieldType fieldType) {
    if (!CONVERTER_MAP.containsKey(fieldType)) {
      throw new UnsupportedOperationException(
          "Conversion from " + fieldType + " to BigDecimal is not supported");
    }

    return CONVERTER_MAP.get(fieldType);
  }
}
