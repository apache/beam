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
package org.apache.beam.dsls.sql.example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.dsls.sql.planner.BeamSqlRunner;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.kafka.BeamKafkaCSVTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * This is one quick example.
 *
 * <p>Before start, follow https://kafka.apache.org/quickstart to setup a Kafka
 * cluster locally, and run below commands to create required Kafka topics:
 * <pre>
 * <code>
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
 *   --partitions 1 --topic orders
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
 *   --partitions 1 --topic sub_orders
 * </code>
 * </pre>
 * After run the application, produce several test records:
 * <pre>
 * <code>
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic orders
 * invalid,record
 * 123445,0,100,3413423
 * 234123,3,232,3451231234
 * 234234,0,5,1234123
 * 345234,0,345234.345,3423
 * </code>
 * </pre>
 * Meanwhile, open another console to see the output:
 * <pre>
 * <code>
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sub_orders
 * **Expected :
 * 123445,0,100.0
 * 345234,0,345234.345
 * </code>
 * </pre>
 */
public class BeamSqlExample implements Serializable {

  public static void main(String[] args) throws Exception {
    BeamSqlRunner runner = new BeamSqlRunner();
    runner.addTable("ORDER_DETAILS", getTable("127.0.0.1:9092", "orders"));
    runner.addTable("SUB_ORDER", getTable("127.0.0.1:9092", "sub_orders"));

    // case 2: insert into <table>(<fields>) select STREAM <fields> from
    // <table> from <clause>
    String sql = "INSERT INTO SUB_ORDER(order_id, site_id, price) " + "SELECT "
        + " order_id, site_id, price " + "FROM ORDER_DETAILS " + "WHERE SITE_ID = 0 and price > 20";

    runner.explainQuery(sql);
    runner.submitQuery(sql);
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
