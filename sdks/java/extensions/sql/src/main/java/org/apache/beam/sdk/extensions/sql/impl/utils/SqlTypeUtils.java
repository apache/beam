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

import com.google.common.base.Optional;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utils to help with SqlTypes.
 */
public class SqlTypeUtils {
  /**
   * Finds an operand with provided type.
   * Returns Optional.absent() if no operand found with matching type
   */
  public static Optional<BeamSqlExpression> findExpressionOfType(
      List<BeamSqlExpression> operands, SqlTypeName type) {

    for (BeamSqlExpression operand : operands) {
      if (type.equals(operand.getOutputType())) {
        return Optional.of(operand);
      }
    }

    return Optional.absent();
  }

  /**
   * Finds an operand with the type in typesToFind.
   * Returns Optional.absent() if no operand found with matching type
   */
  public static Optional<BeamSqlExpression> findExpressionOfType(
      List<BeamSqlExpression> operands, Collection<SqlTypeName> typesToFind) {

    for (BeamSqlExpression operand : operands) {
      if (typesToFind.contains(operand.getOutputType())) {
        return Optional.of(operand);
      }
    }

    return Optional.absent();
  }
}
