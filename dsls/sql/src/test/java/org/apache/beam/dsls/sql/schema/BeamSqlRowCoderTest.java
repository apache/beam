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

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * Tests for BeamSqlRowCoder.
 */
public class BeamSqlRowCoderTest {

  @Test
  public void encodeAndDecode() throws Exception {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("id", SqlTypeName.INTEGER)
            .add("order_id", SqlTypeName.BIGINT)
            .add("price", SqlTypeName.FLOAT)
            .add("amount", SqlTypeName.DOUBLE)
            .add("user_name", SqlTypeName.VARCHAR)
            .build();
      }
    };

    BeamSQLRecordType beamSQLRecordType = BeamSQLRecordType.from(
        protoRowType.apply(new JavaTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT)));
    BeamSQLRow row = new BeamSQLRow(beamSQLRecordType);
    row.addField(0, 1);
    row.addField(1, 1L);
    row.addField(2, 1.1F);
    row.addField(3, 1.1);
    row.addField(4, "hello");

    BeamSqlRowCoder coder = BeamSqlRowCoder.of();
    CoderProperties.coderDecodeEncodeEqual(coder, row);
  }
}
