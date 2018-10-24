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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.row;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** Represents a field access expression. */
public class BeamSqlFieldAccessExpression extends BeamSqlExpression {

  private BeamSqlExpression referenceExpression;
  private int nestedFieldIndex;

  public BeamSqlFieldAccessExpression(
      BeamSqlExpression referenceExpression, int nestedFieldIndex, SqlTypeName nestedFieldType) {

    super(Collections.emptyList(), nestedFieldType);
    this.referenceExpression = referenceExpression;
    this.nestedFieldIndex = nestedFieldIndex;
  }

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    BeamSqlPrimitive targetObject = referenceExpression.evaluate(inputRow, window, env);
    SqlTypeName targetFieldType = targetObject.getOutputType();

    Object targetFieldValue;

    if (SqlTypeName.ARRAY.equals(targetFieldType)) {
      targetFieldValue = ((List) targetObject.getValue()).get(nestedFieldIndex);
    } else if (SqlTypeName.ROW.equals(targetFieldType)) {
      targetFieldValue = ((Row) targetObject.getValue()).getValue(nestedFieldIndex);
    } else {
      throw new IllegalArgumentException(
          "Attempt to access field of unsupported type "
              + targetFieldType.getDeclaringClass().getSimpleName()
              + ". Field access operator is only supported for arrays or rows");
    }

    return BeamSqlPrimitive.of(outputType, targetFieldValue);
  }
}
