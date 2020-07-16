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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelFieldCollation;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

/**
 * Some utility methods for transforming Calcite's constructs into our own Beam constructs (for
 * serialization purpose).
 */
public class CEPUtil {

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

      // check if if the node has quantifier
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
    List<RelFieldCollation> revOrderKeys = orderKeys.getFieldCollations();
    Collections.reverse(revOrderKeys);

    ArrayList<OrderKey> revOrderKeysList = new ArrayList<>();
    for (RelFieldCollation i : revOrderKeys) {
      revOrderKeysList.add(OrderKey.of(i));
    }

    return revOrderKeysList;
  }
}
