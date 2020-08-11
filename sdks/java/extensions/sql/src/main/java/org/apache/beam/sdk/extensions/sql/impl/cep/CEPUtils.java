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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

/**
 * Some utility methods for transforming Calcite's constructs into our own Beam constructs (for
 * serialization purpose).
 */
public class CEPUtils {

  private static Quantifier getQuantifier(int start, int end, boolean isReluctant) {
    Quantifier quantToAdd;
    if (!isReluctant) {
      if (start == end) {
        quantToAdd = new Quantifier("{ " + start + " }");
      } else {
        if (end == -1) {
          if (start == 0) {
            quantToAdd = Quantifier.ASTERISK;
          } else if (start == 1) {
            quantToAdd = Quantifier.PLUS;
          } else {
            quantToAdd = new Quantifier("{ " + start + " }");
          }
        } else {
          if (start == 0 && end == 1) {
            quantToAdd = Quantifier.QMARK;
          } else if (start == -1) {
            quantToAdd = new Quantifier("{ , " + end + " }");
          } else {
            quantToAdd = new Quantifier("{ " + start + " , }");
          }
        }
      }
    } else {
      if (start == end) {
        quantToAdd = new Quantifier("{ " + start + " }?");
      } else {
        if (end == -1) {
          if (start == 0) {
            quantToAdd = Quantifier.ASTERISK_RELUCTANT;
          } else if (start == 1) {
            quantToAdd = Quantifier.PLUS_RELUCTANT;
          } else {
            quantToAdd = new Quantifier("{ " + start + " }?");
          }
        } else {
          if (start == 0 && end == 1) {
            quantToAdd = Quantifier.QMARK_RELUCTANT;
          } else if (start == -1) {
            quantToAdd = new Quantifier("{ , " + end + " }?");
          } else {
            quantToAdd = new Quantifier("{ " + start + " , }?");
          }
        }
      }
    }

    return quantToAdd;
  }

  /** Construct a list of {@code CEPPattern}s from a {@code RexNode}. */
  public static ArrayList<CEPPattern> getCEPPatternFromPattern(
      Schema upStreamSchema, RexNode call, Map<String, RexNode> patternDefs) {
    ArrayList<CEPPattern> patternList = new ArrayList<>();
    if (call.getClass() == RexLiteral.class) {
      String p = ((RexLiteral) call).getValueAs(String.class);
      RexNode pd = patternDefs.get(p);
      patternList.add(CEPPattern.of(upStreamSchema, p, (RexCall) pd, Quantifier.NONE));
    } else {
      RexCall patCall = (RexCall) call;
      SqlOperator operator = patCall.getOperator();
      List<RexNode> operands = patCall.getOperands();

      // check if the node has quantifier
      if (operator.getKind() == SqlKind.PATTERN_QUANTIFIER) {
        String p = ((RexLiteral) operands.get(0)).getValueAs(String.class);
        RexNode pd = patternDefs.get(p);
        int start = ((RexLiteral) operands.get(1)).getValueAs(Integer.class);
        int end = ((RexLiteral) operands.get(2)).getValueAs(Integer.class);
        boolean isReluctant = ((RexLiteral) operands.get(3)).getValueAs(Boolean.class);

        patternList.add(
            CEPPattern.of(upStreamSchema, p, (RexCall) pd, getQuantifier(start, end, isReluctant)));
      } else {
        for (RexNode i : operands) {
          patternList.addAll(getCEPPatternFromPattern(upStreamSchema, i, patternDefs));
        }
      }
    }
    return patternList;
  }

  /** Recursively construct a regular expression from a {@code RexNode}. */
  public static String getRegexFromPattern(RexNode call) {
    if (call.getClass() == RexLiteral.class) {
      return ((RexLiteral) call).getValueAs(String.class);
    } else {
      RexCall opr = (RexCall) call;
      SqlOperator operator = opr.getOperator();
      List<RexNode> operands = opr.getOperands();
      if (operator.getKind() == SqlKind.PATTERN_QUANTIFIER) {
        String p = ((RexLiteral) operands.get(0)).getValueAs(String.class);
        int start = ((RexLiteral) operands.get(1)).getValueAs(Integer.class);
        int end = ((RexLiteral) operands.get(2)).getValueAs(Integer.class);
        boolean isReluctant = ((RexLiteral) operands.get(3)).getValueAs(Boolean.class);
        Quantifier quantifier = getQuantifier(start, end, isReluctant);
        return p + quantifier.toString();
      }
      return getRegexFromPattern(opr.getOperands().get(0))
          + getRegexFromPattern(opr.getOperands().get(1));
    }
  }

  /** Transform a list of keys in Calcite to {@code ORDER BY} to {@code OrderKey}s. */
  public static ArrayList<OrderKey> makeOrderKeysFromCollation(RelCollation orderKeys) {
    List<RelFieldCollation> relOrderKeys = orderKeys.getFieldCollations();

    ArrayList<OrderKey> orderKeysList = new ArrayList<>();
    for (RelFieldCollation i : relOrderKeys) {
      orderKeysList.add(OrderKey.of(i));
    }

    return orderKeysList;
  }

  /** Transform the partition columns into serializable CEPFieldRef. */
  public static List<CEPFieldRef> getCEPFieldRefFromParKeys(List<RexNode> parKeys) {
    ArrayList<CEPFieldRef> fieldList = new ArrayList<>();
    for (RexNode i : parKeys) {
      RexInputRef parKey = (RexInputRef) i;
      fieldList.add(new CEPFieldRef(parKey.getName(), parKey.getIndex()));
    }
    return fieldList;
  }

  /** a function that finds a pattern reference recursively. */
  public static CEPFieldRef getFieldRef(CEPOperation opr) {
    if (opr.getClass() == CEPFieldRef.class) {
      CEPFieldRef field = (CEPFieldRef) opr;
      return field;
    } else if (opr.getClass() == CEPCall.class) {
      CEPCall call = (CEPCall) opr;
      CEPFieldRef field;

      for (CEPOperation i : call.getOperands()) {
        field = getFieldRef(i);
        if (field != null) {
          return field;
        }
      }
      return null;
    } else {
      return null;
    }
  }

  public static Schema.FieldType getFieldType(Schema streamSchema, CEPOperation measureOperation) {

    if (measureOperation.getClass() == CEPFieldRef.class) {
      CEPFieldRef field = (CEPFieldRef) measureOperation;
      return streamSchema.getField(field.getIndex()).getType();
    } else if (measureOperation.getClass() == CEPCall.class) {

      CEPCall call = (CEPCall) measureOperation;
      CEPKind oprKind = call.getOperator().getCepKind();

      if (oprKind == CEPKind.SUM || oprKind == CEPKind.COUNT) {
        return Schema.FieldType.INT32;
      } else if (oprKind == CEPKind.AVG) {
        return Schema.FieldType.DOUBLE;
      }
      CEPFieldRef refOpt;
      for (CEPOperation i : call.getOperands()) {
        refOpt = getFieldRef(i);
        if (refOpt != null) {
          return streamSchema.getField(refOpt.getIndex()).getType();
        }
      }
      throw new UnsupportedOperationException("the function in Measures is not recognized.");
    } else {
      throw new UnsupportedOperationException("the function in Measures is not recognized.");
    }
  }
}
