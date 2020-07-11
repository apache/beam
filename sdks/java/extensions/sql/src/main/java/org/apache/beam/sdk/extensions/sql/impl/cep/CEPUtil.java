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
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;

public class CEPUtil {
  // construct a list of ceppatterns from the rexnode
  public static ArrayList<CEPPattern> getCEPPatternFromPattern(
      Schema mySchema, RexNode call, Map<String, RexNode> patternDefs) {
    ArrayList<CEPPattern> patternList = new ArrayList<>();
    if (call.getClass() == RexLiteral.class) {
      String p = ((RexLiteral) call).getValueAs(String.class);
      RexNode pd = patternDefs.get(p);
      patternList.add(CEPPattern.of(mySchema, p, (RexCall) pd));
    } else {
      RexCall oprs = (RexCall) call;
      patternList.addAll(
          getCEPPatternFromPattern(mySchema, oprs.getOperands().get(0), patternDefs));
      patternList.addAll(
          getCEPPatternFromPattern(mySchema, oprs.getOperands().get(1), patternDefs));
    }
    return patternList;
  }

  // recursively change a RexNode into a regular expr
  // TODO: support quantifiers: PATTERN_QUANTIFIER('A', 1, -1, false) false?
  public static String getRegexFromPattern(RexNode call) {
    if (call.getClass() == RexLiteral.class) {
      return ((RexLiteral) call).getValueAs(String.class);
    } else {
      RexCall oprs = (RexCall) call;
      return getRegexFromPattern(oprs.getOperands().get(0))
          + getRegexFromPattern(oprs.getOperands().get(1));
    }
  }
}
