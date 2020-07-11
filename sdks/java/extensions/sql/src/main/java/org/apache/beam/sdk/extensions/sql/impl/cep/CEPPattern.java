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
import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;

// a pattern class that stores the definition of a single pattern
public class CEPPattern implements Serializable {

  private final Schema mySchema;
  private final String patternVar;
  private final PatternCondition patternCondition;
  private final Quantifier quant;

  private CEPPattern(Schema mySchema, String patternVar, @Nullable RexCall patternDef) {

    this.mySchema = mySchema;
    this.patternVar = patternVar;
    this.quant = Quantifier.NONE;

    if (patternDef == null) {
      this.patternCondition =
          new PatternCondition(this) {
            @Override
            public boolean eval(Row eleRow) {
              return true;
            }
          };
      return;
    }

    CEPCall cepCall = CEPCall.of(patternDef);
    CEPOperator cepOperator = cepCall.getOperator();
    List<CEPOperation> cepOperands = cepCall.getOperands();
    CEPCall cepOpr0 = (CEPCall) cepOperands.get(0);
    CEPLiteral cepOpr1 = (CEPLiteral) cepOperands.get(1);

    switch (cepOperator.getCepKind()) {
      case EQUALS:
        this.patternCondition =
            new PatternCondition(this) {
              @Override
              public boolean eval(Row eleRow) {
                return evalOperation(cepOpr0, cepOpr1, eleRow) == 0;
              }
            };
        break;
      case GREATER_THAN:
        this.patternCondition =
            new PatternCondition(this) {
              @Override
              public boolean eval(Row eleRow) {
                return evalOperation(cepOpr0, cepOpr1, eleRow) > 0;
              }
            };
        break;
      case GREATER_THAN_OR_EQUAL:
        this.patternCondition =
            new PatternCondition(this) {
              @Override
              public boolean eval(Row eleRow) {
                return evalOperation(cepOpr0, cepOpr1, eleRow) >= 0;
              }
            };
        break;
      case LESS_THAN:
        this.patternCondition =
            new PatternCondition(this) {
              @Override
              public boolean eval(Row eleRow) {
                return evalOperation(cepOpr0, cepOpr1, eleRow) < 0;
              }
            };
        break;
      case LESS_THAN_OR_EQUAL:
        this.patternCondition =
            new PatternCondition(this) {
              @Override
              public boolean eval(Row eleRow) {
                return evalOperation(cepOpr0, cepOpr1, eleRow) <= 0;
              }
            };
        break;
      default:
        throw new SqlConversionException("Comparison operator not recognized.");
    }
  }

  // Last(*.$1, 1) || NEXT(*.$1, 0)
  private int evalOperation(CEPCall operation, CEPLiteral lit, Row rowEle) {
    CEPOperator call = operation.getOperator();
    List<CEPOperation> operands = operation.getOperands();

    if (call.getCepKind() == CEPKind.LAST) { // support only simple match for now: LAST(*.$, 0)
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
            throw new SqlConversionException("specified column not comparable");
        }
      }
    }
    throw new SqlConversionException("backward functions (PREV, NEXT) not supported for now");
  }

  public boolean evalRow(Row rowEle) {
    return patternCondition.eval(rowEle);
  }

  @Override
  public String toString() {
    return patternVar + quant.toString();
  }

  public static CEPPattern of(Schema theSchema, String patternVar, RexCall patternDef) {
    return new CEPPattern(theSchema, patternVar, patternDef);
  }
}
