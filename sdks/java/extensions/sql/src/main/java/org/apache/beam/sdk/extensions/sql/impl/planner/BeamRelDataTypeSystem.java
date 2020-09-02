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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;

/** customized data type in Beam. */
public class BeamRelDataTypeSystem extends RelDataTypeSystemImpl {
  public static final RelDataTypeSystem INSTANCE = new BeamRelDataTypeSystem();

  private BeamRelDataTypeSystem() {}

  @Override
  public int getMaxNumericScale() {
    return 38;
  }

  @Override
  public int getMaxNumericPrecision() {
    return 38;
  }

  /* operators that change string length should return varchar. */
  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return true;
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case TIME:
        return 6; // support microsecond time precision
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return 6; // support microsecond datetime precision
      default:
        return super.getMaxPrecision(typeName);
    }
  }
}
