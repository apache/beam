package org.beam.sdk.java.sql.examples;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.beam.sdk.java.sql.planner.BeamQueryPlanner;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.kafka.BeamKafkaTable;

import com.google.common.collect.ImmutableList;

public class BeamSqlExample implements Serializable{
  private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final SchemaPlus schema = Frameworks.createRootSchema(true);

  public void addSchema(String schemaName, Schema scheme) {
    schema.add(schemaName, schema);
  }

  public void addTable(String tableName, Table table) {
    schema.add(tableName, table);
  }

  public void explainAndRun(String sqlString) throws IOException, SQLException {

    BeamQueryPlanner planner = new BeamQueryPlanner(schema);
    try {
      System.out.println("SQL>: \n" + sqlString);
      planner.compileAndRun(sqlString);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException, SQLException {
    BeamSqlExample runner = new BeamSqlExample();
    runner.initTables();
    
    // case 2: insert into <table>(<fields>) select STREAM <fields> from
    // <table> from <clause>
    String sql = "SELECT " + " SITEID, PAGEID as new_pageId " + "FROM SOJ_EVENT "
        + "WHERE SITEID > 0 ";
    
    runner.explainAndRun(sql);
  }

  void initTables() {
    addTable("SOJ_EVENT",
        "rheos-kafka-proxy-1.lvs02.dev.ebayc3.com:9092,rheos-kafka-proxy-2.lvs02.dev.ebayc3.com:9092",
        "behavior.pulsar.sojevent.total");
    addTable("subrowverevent", "flink2-8332.lvs01.dev.ebayc3.com:9092", "subrowverevent");
  }

  void addTable(String tableName, String bootstrapServer, String topic) {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("EVENTTIMESTAMP", SqlTypeName.TIMESTAMP)
            .add("ITEMID", SqlTypeName.BIGINT).add("SITEID", SqlTypeName.INTEGER)
            .add("PAGEID", SqlTypeName.INTEGER).add("PAGENAME", SqlTypeName.VARCHAR)
            .build();
      }
    };
    Direction dir = Direction.ASCENDING;
    RelFieldCollation collation = new RelFieldCollation(0, dir, NullDirection.UNSPECIFIED);
    Statistic stat = Statistics.of(5, ImmutableList.of(ImmutableBitSet.of(0)),
        ImmutableList.of(RelCollations.of(collation)));
    
    stat = Statistics.of(100d,
        ImmutableList.<ImmutableBitSet>of(),
RelCollations.createSingleton(0));
    
    stat = Statistics.UNKNOWN;
    
    Map<String, Object> consumerPara = new HashMap<String, Object>();
    consumerPara.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // latest
    // or
    // earliest
    consumerPara.put(ConsumerConfig.GROUP_ID_CONFIG, "toraframeworktest_sojeventproxy");
    consumerPara.put(ConsumerConfig.CLIENT_ID_CONFIG, "107b877c-2694-4298-beed-9007f54602aa");
    
    addTable(tableName,
        new BeamKafkaTable(protoRowType, new RheosSourceTransform(BeamSQLRecordType.from(protoRowType.apply(new JavaTypeFactoryImpl()))), null, bootstrapServer, Arrays.asList(topic))
          .updateConsumerProperties(consumerPara)
          );
  }
}
