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
  public static Optional<SqlTypeName> findExpressionOfType(
      List<BeamSqlExpression> operands, SqlTypeName type) {

    for (BeamSqlExpression operand : operands) {
      if (type.equals(operand.getOutputType())) {
        return Optional.of(operand.getOutputType());
      }
    }

    return Optional.absent();
  }

  /**
   * Finds an operand with the type in typesToFind.
   * Returns Optional.absent() if no operand found with matching type
   */
  public static Optional<SqlTypeName> findExpressionOfType(
      List<BeamSqlExpression> operands, Collection<SqlTypeName> typesToFind) {

    for (BeamSqlExpression operand : operands) {
      if (typesToFind.contains(operand.getOutputType())) {
        return Optional.of(operand.getOutputType());
      }
    }

    return Optional.absent();
  }
}
