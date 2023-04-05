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
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;

/** Core pattern class that stores the definition of a single pattern. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CEPPattern implements Serializable {

  private final String patternVar;
  private final CEPCall patternCondition;
  private final Quantifier quant;

  private CEPPattern(String patternVar, @Nullable RexCall patternDef, Quantifier quant) {

    this.patternVar = patternVar;
    this.quant = quant;

    if (patternDef == null) {
      this.patternCondition = null;
      return;
    }

    this.patternCondition = CEPCall.of(patternDef);
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
    return new CEPPattern(patternVar, patternDef, quant);
  }
}
