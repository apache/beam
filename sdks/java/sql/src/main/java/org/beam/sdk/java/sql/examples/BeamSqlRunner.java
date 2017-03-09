package org.beam.sdk.java.sql.examples;

import java.io.IOException;
import java.sql.SQLException;

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
import org.beam.sdk.java.sql.planner.BeamQueryPlanner;

import com.google.common.collect.ImmutableList;

public class BeamSqlRunner {
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
    BeamSqlRunner runner = new BeamSqlRunner();
    runner.initTables();
    
    // case 2: insert into <table>(<fields>) select STREAM <fields> from
    // <table> from <clause>
    String sql = "SELECT " + " SITEID, PAGEID as new_pageId,floor(EVENTTIMESTAMP TO HOUR) AS EVENT_HOUR"
        + ", COUNT(*) AS SIZE " + "FROM SOJ_EVENT "
        + "WHERE SITEID >= 0 " + "GROUP BY SITEID, PAGEID, floor(EVENTTIMESTAMP TO HOUR)";
    
    sql = "SELECT " + " SITEID, PAGEID, HOP_START(EVENTTIMESTAMP, INTERVAL '1' HOUR, INTERVAL '3' HOUR) "
        + ", COUNT(*) AS SIZE " + "FROM SOJ_EVENT "
        + "WHERE SITEID >= 0 " + "GROUP BY SITEID, PAGEID, HOP(EVENTTIMESTAMP, INTERVAL '1' HOUR, INTERVAL '3' HOUR) ";
    
    runner.explainAndRun(sql);
  }

  void initTables() {
    addTable("SOJ_EVENT",
        "rheos-kafka-proxy-1.lvs02.dev.ebayc3.com:9092,rheos-kafka-proxy-2.lvs02.dev.ebayc3.com:9092",
        "behavior.pulsar.sojevent.total");
    addTable("subrowverevent", "flink2-8332.lvs01.dev.ebayc3.com:9092", "subrowverevent");
    // addTable("externalorder", "flink2-8332.lvs01.dev.ebayc3.com:9092",
    // "externalorder");
  }

  void addTable(String tableName, String bootstrapServer, String topic) {
    RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder().add("EVENTTIMESTAMP", SqlTypeName.TIMESTAMP)
            .add("ITEMID", SqlTypeName.BIGINT).add("SITEID", SqlTypeName.INTEGER)
            .add("PAGEID", SqlTypeName.INTEGER).add("PAGENAME", SqlTypeName.VARCHAR)
            // .add("APPLICATIONPAYLOAD", SqlTypeName.INTEGER)
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
    
//    addTable(tableName,
//        new BeamKafkaTable(protoRowType, stat, bootstrapServer, topic));
  }
}
