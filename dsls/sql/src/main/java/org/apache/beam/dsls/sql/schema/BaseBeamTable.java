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
package org.apache.beam.dsls.sql.schema;

import java.io.Serializable;
import org.apache.beam.dsls.sql.planner.BeamQueryPlanner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;

/**
 * Each IO in Beam has one table schema, by extending {@link BaseBeamTable}.
 */
public abstract class BaseBeamTable implements ScannableTable, Serializable {

  /**
   *
   */
  private static final long serialVersionUID = -1262988061830914193L;
  private RelDataType relDataType;

  protected BeamSQLRecordType beamSqlRecordType;

  public BaseBeamTable(RelProtoDataType protoRowType) {
    this.relDataType = protoRowType.apply(BeamQueryPlanner.TYPE_FACTORY);
    this.beamSqlRecordType = BeamSQLRecordType.from(relDataType);
  }

  /**
   * In Beam SQL, there's no difference between a batch query and a streaming
   * query. {@link BeamIOType} is used to validate the sources.
   */
  public abstract BeamIOType getSourceType();

  /**
   * create a {@code IO.read()} instance to read from source.
   *
   */
  public abstract PTransform<? super PBegin, PCollection<BeamSQLRow>> buildIOReader();

  /**
   * create a {@code IO.write()} instance to write to target.
   *
   */
  public abstract PTransform<? super PCollection<BeamSQLRow>, PDone> buildIOWriter();

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    // not used as Beam SQL uses its own execution engine
    return null;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return relDataType;
  }

  /**
   * Not used {@link Statistic} to optimize the plan.
   */
  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  /**
   * all sources are treated as TABLE in Beam SQL.
   */
  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

}
