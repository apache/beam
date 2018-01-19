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

package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Provides converters from {@link BigDecimal} to other numeric types based on
 * the input {@link SqlTypeName}.
 */
public class BigDecimalConverter {

  private static final Map<SqlTypeName, SerializableFunction<BigDecimal, ? extends Number>>
      CONVERTER_MAP = ImmutableMap
      .<SqlTypeName, SerializableFunction<BigDecimal, ? extends Number>>builder()
      .put(SqlTypeName.INTEGER, BigDecimal::intValue)
      .put(SqlTypeName.SMALLINT, BigDecimal::shortValue)
      .put(SqlTypeName.TINYINT, BigDecimal::byteValue)
      .put(SqlTypeName.BIGINT, BigDecimal::longValue)
      .put(SqlTypeName.FLOAT, BigDecimal::floatValue)
      .put(SqlTypeName.DOUBLE, BigDecimal::doubleValue)
      .put(SqlTypeName.DECIMAL, v -> v)
      .build();

  public static SerializableFunction<BigDecimal, ? extends Number> forSqlType(
      SqlTypeName sqlTypeName) {

    if (!CONVERTER_MAP.containsKey(sqlTypeName)) {
      throw new UnsupportedOperationException(
          "Conversion from " + sqlTypeName + " to BigDecimal is not supported");
    }

    return CONVERTER_MAP.get(sqlTypeName);
  }
}
