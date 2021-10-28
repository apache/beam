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
package org.apache.beam.sdk.extensions.sql.impl;

import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/** This is a class to indicate that a TVF is a ZetaSQL SQL native UDTVF. */
@Internal
public class ZetaSqlUserDefinedSQLNativeTableValuedFunction extends SqlUserDefinedFunction {

  public ZetaSqlUserDefinedSQLNativeTableValuedFunction(
      SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function) {
    super(
        opName,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        paramTypes,
        function);
  }
}
