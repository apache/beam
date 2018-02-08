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
package org.apache.beam.sdk.extensions.sql.impl.transform;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionExecutor;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamProjectRel;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

/**
 * {@code BeamSqlProjectFn} is the executor for a {@link BeamProjectRel} step.
 */
public class BeamSqlProjectFn extends DoFn<Row, Row> {
  private String stepName;
  private BeamSqlExpressionExecutor executor;
  private RowType outputRowType;

  public BeamSqlProjectFn(
      String stepName, BeamSqlExpressionExecutor executor,
      RowType outputRowType) {
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
    Row inputRow = c.element();
    List<Object> rawResultValues = executor.execute(inputRow, window);

    List<Object> castResultValues =
        IntStream
            .range(0, outputRowType.getFieldCount())
            .mapToObj(i -> castField(rawResultValues, i))
            .collect(toList());

    c.output(
        Row
            .withRowType(outputRowType)
            .addValues(castResultValues)
            .build());
  }

  private Object castField(List<Object> resultValues, int i) {
    return BeamTableUtils
        .autoCastField(outputRowType.getFieldCoder(i), resultValues.get(i));
  }

  @Teardown
  public void close() {
    executor.close();
  }

}
