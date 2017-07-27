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
package org.apache.beam.dsls.sql.transform;

import java.util.List;
import org.apache.beam.dsls.sql.interpreter.BeamSqlExpressionExecutor;
import org.apache.beam.dsls.sql.rel.BeamProjectRel;
import org.apache.beam.dsls.sql.schema.BeamRow;
import org.apache.beam.dsls.sql.schema.BeamRowType;
import org.apache.beam.dsls.sql.schema.BeamTableUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 *
 * {@code BeamSqlProjectFn} is the executor for a {@link BeamProjectRel} step.
 *
 */
public class BeamSqlProjectFn extends DoFn<BeamRow, BeamRow> {
  private String stepName;
  private BeamSqlExpressionExecutor executor;
  private BeamRowType outputRowType;

  public BeamSqlProjectFn(String stepName, BeamSqlExpressionExecutor executor,
      BeamRowType outputRowType) {
    super();
    this.stepName = stepName;
    this.executor = executor;
    this.outputRowType = outputRowType;
  }

  @Setup
  public void setup() {
    executor.prepare();
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    BeamRow inputRow = c.element();
    List<Object> results = executor.execute(inputRow);

    BeamRow outRow = new BeamRow(outputRowType);
    outRow.updateWindowRange(inputRow, window);

    for (int idx = 0; idx < results.size(); ++idx) {
      BeamTableUtils.addFieldWithAutoTypeCasting(outRow, idx, results.get(idx));
    }

    c.output(outRow);
  }

  @Teardown
  public void close() {
    executor.close();
  }

}
