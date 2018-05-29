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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionExecutor;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamFilterRel;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;

/** {@code BeamSqlFilterFn} is the executor for a {@link BeamFilterRel} step. */
public class BeamSqlFilterFn extends DoFn<Row, Row> {

  private String stepName;
  private BeamSqlExpressionExecutor executor;

  public BeamSqlFilterFn(String stepName, BeamSqlExpressionExecutor executor) {
    super();
    this.stepName = stepName;
    this.executor = executor;
  }

  @Setup
  public void setup() {
    executor.prepare();
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) {
    Row in = c.element();

    List<Object> result = executor.execute(in, window, ImmutableMap.of());

    if ((Boolean) result.get(0)) {
      c.output(in);
    }
  }

  @Teardown
  public void close() {
    executor.close();
  }
}
