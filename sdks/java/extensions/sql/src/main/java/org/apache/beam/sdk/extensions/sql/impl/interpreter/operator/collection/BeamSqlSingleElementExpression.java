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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.collection;

import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Implements ELEMENT(collection) operation which returns the single element from the collection.
 *
 * <p>Throws if there is more than one element. Returns null if collection is empty.
 */
public class BeamSqlSingleElementExpression extends BeamSqlExpression {

  public BeamSqlSingleElementExpression(
      List<BeamSqlExpression> operands,
      SqlTypeName sqlTypeName) {

    super(operands, sqlTypeName);
  }

  @Override
  public boolean accept() {
    return operands.size() == 1;
  }

  @Override
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    Collection<Object> collection = opValueEvaluated(0, inputRow, window);

    if (collection.size() <= 1) {
      return (collection.size() == 0)
          ? BeamSqlPrimitive.of(outputType, null)
          : BeamSqlPrimitive.of(outputType, collection.iterator().next());
    }

    throw new IllegalArgumentException(
        "ELEMENT expression accepts either empty collections "
        + "or collections with a single element. "
        + "Received collection with "
        + collection.size()
        + " elements");
  }
}
