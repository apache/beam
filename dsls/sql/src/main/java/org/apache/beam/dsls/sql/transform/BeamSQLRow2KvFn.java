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

import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Transform a {@code BeamSQLRow} to a {@code KV<BeamSQLRow, BeamSQLRow>}.
 */
public class BeamSQLRow2KvFn extends SimpleFunction<BeamSQLRow, KV<BeamSQLRow, BeamSQLRow>> {
  private boolean useEmptyRow;
  public BeamSQLRow2KvFn(boolean useEmptyRow) {
    this.useEmptyRow = useEmptyRow;
  }
  @Override public KV<BeamSQLRow, BeamSQLRow> apply(BeamSQLRow input) {
    if (useEmptyRow) {
      return KV.of(input, BeamSQLRow.EMPTY_ROW);
    } else {
      return KV.of(input, input);
    }
  }
}
