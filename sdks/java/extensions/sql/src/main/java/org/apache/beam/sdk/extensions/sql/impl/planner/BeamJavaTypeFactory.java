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

import java.lang.reflect.Type;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.BasicSqlType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;

/** customized data type in Beam. */
public class BeamJavaTypeFactory extends JavaTypeFactoryImpl {
  public static final JavaTypeFactory INSTANCE = new BeamJavaTypeFactory();

  private BeamJavaTypeFactory() {
    super(BeamRelDataTypeSystem.INSTANCE);
  }

  @Override
  public Type getJavaClass(RelDataType type) {
    if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
      if (type.getSqlTypeName() == SqlTypeName.FLOAT) {
        return type.isNullable() ? Float.class : float.class;
      }
    }
    return super.getJavaClass(type);
  }
}
