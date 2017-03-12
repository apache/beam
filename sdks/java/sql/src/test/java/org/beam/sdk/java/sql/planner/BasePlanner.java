package org.beam.sdk.java.sql.planner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.beam.sdk.java.sql.schema.BaseBeamTable;
import org.beam.sdk.java.sql.schema.kafka.BeamKafkaCSVTable;
import org.junit.BeforeClass;

public class BasePlanner {
  public static BeamSqlRunner runner = new BeamSqlRunner();

  @BeforeClass
  public static void prepare(){
    runner.addTable("ORDER_DETAILS", getTable("127.0.0.1:9092", "orders"));
    runner.addTable("SUB_ORDER", getTable("127.0.0.1:9092", "sub_orders"));
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
