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
package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.CalcRelSplitter;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcSplittingRule;

/**
 * A {@link BeamCalcSplittingRule} that converts a {@link LogicalCalc} to a chain of {@link
 * BeamZetaSqlCalcRel} and/or {@link BeamCalcRel} via {@link CalcRelSplitter}.
 *
 * <p>Only Java UDFs are implemented using {@link BeamCalcRel}. All other expressions are
 * implemented using {@link BeamZetaSqlCalcRel}.
 */
public class BeamZetaSqlCalcSplittingRule extends BeamCalcSplittingRule {
  public static final BeamZetaSqlCalcSplittingRule INSTANCE = new BeamZetaSqlCalcSplittingRule();

  private BeamZetaSqlCalcSplittingRule() {
    super("BeamZetaSqlCalcRule");
  }

  @Override
  protected CalcRelSplitter.RelType[] getRelTypes() {
    return new CalcRelSplitter.RelType[] {
      new BeamZetaSqlRelType("BeamZetaSqlRelType"), new BeamCalcRelType("BeamCalcRelType")
    };
  }
}
