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
import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;

/** Core pattern class that stores the definition of a single pattern. */
public class CEPPattern implements Serializable {

  private final Schema mySchema;
  private final String patternVar;
  private final CEPCall patternCondition;
  private final Quantifier quant;

  private CEPPattern(
      Schema mySchema, String patternVar, @Nullable RexCall patternDef, Quantifier quant) {

    this.mySchema = mySchema;
    this.patternVar = patternVar;
    this.quant = quant;

    if (patternDef == null) {
      this.patternCondition = null;
      return;
    }

    this.patternCondition = CEPCall.of(patternDef);
  }

  // support only simple match: LAST(*.$, 0) for now.
  // TODO: remove this method after implementing NFA
  private int evalOperation(CEPCall operation, CEPLiteral lit, Row rowEle) {
    CEPOperator call = operation.getOperator();
    List<CEPOperation> operands = operation.getOperands();

    if (call.getCepKind() == CEPKind.LAST) {
      CEPOperation opr0 = operands.get(0);
      CEPLiteral opr1 = (CEPLiteral) operands.get(1);
      if (opr0.getClass() == CEPFieldRef.class && opr1.getDecimal().equals(BigDecimal.ZERO)) {
        int fIndex = ((CEPFieldRef) opr0).getIndex();
        Schema.Field fd = mySchema.getField(fIndex);
        Schema.FieldType dtype = fd.getType();

        switch (dtype.getTypeName()) {
          case BYTE:
            return rowEle.getByte(fIndex).compareTo(lit.getByte());
          case INT16:
            return rowEle.getInt16(fIndex).compareTo(lit.getInt16());
          case INT32:
            return rowEle.getInt32(fIndex).compareTo(lit.getInt32());
          case INT64:
            return rowEle.getInt64(fIndex).compareTo(lit.getInt64());
          case DECIMAL:
            return rowEle.getDecimal(fIndex).compareTo(lit.getDecimal());
          case FLOAT:
            return rowEle.getFloat(fIndex).compareTo(lit.getFloat());
          case DOUBLE:
            return rowEle.getDouble(fIndex).compareTo(lit.getDouble());
          case STRING:
            return rowEle.getString(fIndex).compareTo(lit.getString());
          case DATETIME:
            return rowEle.getDateTime(fIndex).compareTo(lit.getDateTime());
          case BOOLEAN:
            return rowEle.getBoolean(fIndex).compareTo(lit.getBoolean());
          default:
            throw new UnsupportedOperationException(
                "Specified column not comparable: " + fd.getName());
        }
      }
    }
    throw new UnsupportedOperationException(
        "backward functions (PREV, NEXT) not supported for now");
  }

  public boolean evalRow(Row rowEle) {
    return patternCondition.eval(rowEle);
  }

  @Override
  public String toString() {
    return patternVar + quant.toString();
  }

  public CEPCall getPatternCondition() {
    return patternCondition;
  }

  public String getPatternVar() {
    return patternVar;
  }

  public Quantifier getQuantifier() {
    return quant;
  }

  public static CEPPattern of(
      Schema theSchema, String patternVar, RexCall patternDef, Quantifier quant) {
    return new CEPPattern(theSchema, patternVar, patternDef, quant);
  }
}
