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

import java.math.BigDecimal;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;

/**
 * Provides converters from {@link BigDecimal} to other numeric types based on the input {@link
 * TypeName}.
 */
public class BigDecimalConverter {

  private static final Map<TypeName, SerializableFunction<BigDecimal, ? extends Number>>
      CONVERTER_MAP =
          ImmutableMap.<TypeName, SerializableFunction<BigDecimal, ? extends Number>>builder()
              .put(TypeName.INT32, BigDecimal::intValue)
              .put(TypeName.INT16, BigDecimal::shortValue)
              .put(TypeName.BYTE, BigDecimal::byteValue)
              .put(TypeName.INT64, BigDecimal::longValue)
              .put(TypeName.FLOAT, BigDecimal::floatValue)
              .put(TypeName.DOUBLE, BigDecimal::doubleValue)
              .put(TypeName.DECIMAL, v -> v)
              .build();

  public static SerializableFunction<BigDecimal, ? extends Number> forSqlType(TypeName typeName) {
    if (!CONVERTER_MAP.containsKey(typeName)) {
      throw new UnsupportedOperationException(
          "Conversion from " + typeName + " to BigDecimal is not supported");
    }

    return CONVERTER_MAP.get(typeName);
  }
}
