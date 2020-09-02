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
package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexPatternFieldRef;

/**
 * {@code CEPOperation} is the base class for the evaluation operations defined in the {@code
 * DEFINE} syntax of {@code MATCH_RECOGNIZE}. {@code CEPCall}, {@code CEPFieldRef}, {@code
 * CEPLiteral} are the subclasses of it.
 */
public abstract class CEPOperation implements Serializable {

  public static CEPOperation of(RexNode operation) {
    if (operation.getClass() == RexCall.class) {
      RexCall call = (RexCall) operation;
      return CEPCall.of(call);
    } else if (operation.getClass() == RexLiteral.class) {
      RexLiteral lit = (RexLiteral) operation;
      return CEPLiteral.of(lit);
    } else if (operation.getClass() == RexPatternFieldRef.class) {
      RexPatternFieldRef fieldRef = (RexPatternFieldRef) operation;
      return CEPFieldRef.of(fieldRef);
    } else {
      throw new SqlConversionException("RexNode not supported: " + operation.getClass().getName());
    }
  }
}
