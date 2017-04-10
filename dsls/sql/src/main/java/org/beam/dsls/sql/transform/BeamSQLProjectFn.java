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
package org.beam.dsls.sql.transform;

import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.beam.dsls.sql.interpreter.BeamSQLExpressionExecutor;
import org.beam.dsls.sql.rel.BeamProjectRel;
import org.beam.dsls.sql.schema.BeamSQLRecordType;
import org.beam.dsls.sql.schema.BeamSQLRow;

/**
 *
 * {@code BeamSQLProjectFn} is the executor for a {@link BeamProjectRel} step.
 *
 */
public class BeamSQLProjectFn extends DoFn<BeamSQLRow, BeamSQLRow> {

  /**
   *
   */
  private static final long serialVersionUID = -1046605249999014608L;
  private String stepName;
  private BeamSQLExpressionExecutor executor;
  private BeamSQLRecordType outputRecordType;

  public BeamSQLProjectFn(String stepName, BeamSQLExpressionExecutor executor,
      BeamSQLRecordType outputRecordType) {
    super();
    this.stepName = stepName;
    this.executor = executor;
    this.outputRecordType = outputRecordType;
  }

  @Setup
  public void setup() {
    executor.prepare();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    List<Object> results = executor.execute(c.element());

    BeamSQLRow outRow = new BeamSQLRow(outputRecordType);
    for (int idx = 0; idx < results.size(); ++idx) {
      outRow.addField(idx, results.get(idx));
    }

    c.output(outRow);
  }

  @Teardown
  public void close() {
    executor.close();
  }

}
