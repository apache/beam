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
package org.apache.beam.dsls.sql.planner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.kafka.BeamKafkaCSVTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.BeforeClass;

/**
 * prepare {@code BeamSqlRunner} for test.
 *
 */
public class BasePlanner {
  public static BeamSqlRunner runner = new BeamSqlRunner();

  @BeforeClass
  public static void prepare() {
    runner.addTable("ORDER_DETAILS", getTable());
    runner.addTable("SUB_ORDER", getTable("127.0.0.1:9092", "sub_orders"));
    runner.addTable("SUB_ORDER_RAM", getTable());
  }

  private static BaseBeamTable getTable() {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("order_id", SqlTypeName.BIGINT).add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE).add("order_time", SqlTypeName.TIMESTAMP).build();
      }
    };

    return new MockedBeamSQLTable(protoRowType);
  }

  public static BaseBeamTable getTable(String bootstrapServer, String topic) {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("order_id", SqlTypeName.BIGINT).add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE).add("order_time", SqlTypeName.TIMESTAMP).build();
      }
    };

    Map<String, Object> consumerPara = new HashMap<String, Object>();
    consumerPara.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    return new BeamKafkaCSVTable(protoRowType, bootstrapServer, Arrays.asList(topic))
        .updateConsumerProperties(consumerPara);
  }
}
