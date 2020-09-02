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
package org.apache.beam.sdk.extensions.sql.meta;

import java.util.List;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;

/** Basic implementation of {@link BeamSqlTable} methods used by predicate and filter push-down. */
public abstract class BaseBeamTable implements BeamSqlTable {

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    String error = "%s does not support predicate/project push-down, yet non-empty %s is passed.";

    if (!(filters instanceof DefaultTableFilter)) {
      throw new UnsupportedOperationException(
          String.format(error, this.getClass().getName(), "'filters'"));
    }

    if (!fieldNames.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(error, this.getClass().getName(), "'fieldNames'"));
    }

    return buildIOReader(begin);
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    return new DefaultTableFilter(filter);
  }

  @Override
  public ProjectSupport supportsProjects() {
    return ProjectSupport.NONE;
  }
}
