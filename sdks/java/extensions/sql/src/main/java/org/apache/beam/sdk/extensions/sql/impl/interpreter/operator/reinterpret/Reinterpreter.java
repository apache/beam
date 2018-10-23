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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret;

import com.google.common.base.Optional;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;

/** Class that tracks conversions between SQL types. */
public class Reinterpreter {

  /** Builder for Reinterpreter. */
  public static class Builder {

    private Map<SqlTypeName, Map<SqlTypeName, ReinterpretConversion>> conversions = new HashMap<>();

    public Builder withConversion(ReinterpretConversion conversion) {
      Set<SqlTypeName> fromTypes = conversion.from();
      SqlTypeName toType = conversion.to();

      for (SqlTypeName fromType : fromTypes) {
        if (!conversions.containsKey(fromType)) {
          conversions.put(fromType, new HashMap<>());
        }

        conversions.get(fromType).put(toType, conversion);
      }

      return this;
    }

    public Reinterpreter build() {
      if (conversions.isEmpty()) {
        throw new IllegalArgumentException("Conversions should not be empty");
      }

      return new Reinterpreter(this);
    }
  }

  private Map<SqlTypeName, Map<SqlTypeName, ReinterpretConversion>> conversions;

  private Reinterpreter(Builder builder) {
    this.conversions = builder.conversions;
  }

  public static Builder builder() {
    return new Builder();
  }

  public boolean canConvert(SqlTypeName from, SqlTypeName to) {
    return getConversion(from, to).isPresent();
  }

  public BeamSqlPrimitive convert(SqlTypeName to, BeamSqlPrimitive value) {
    Optional<ReinterpretConversion> conversion = getConversion(value.getOutputType(), to);
    if (!conversion.isPresent()) {
      throw new UnsupportedOperationException(
          "Unsupported conversion: " + value.getOutputType().name() + "->" + to.name());
    }

    return conversion.get().convert(value);
  }

  private Optional<ReinterpretConversion> getConversion(SqlTypeName from, SqlTypeName to) {
    if (!conversions.containsKey(from)) {
      return Optional.absent();
    }

    Map<SqlTypeName, ReinterpretConversion> allConversionsFrom = conversions.get(from);

    ReinterpretConversion conversionTo = allConversionsFrom.get(to);

    return Optional.fromNullable(conversionTo);
  }
}
