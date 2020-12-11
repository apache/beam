package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.io.cdc.KafkaSourceConsumerFn.OffsetHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CounterSourceConnectorTest {

//  @Test
//  public void testQuickCount() {
//    Pipeline p = Pipeline.create();
//    PCollection<Integer> counts = p.apply(
//        Create
//            .of(Lists.newArrayList((Map<String, String>) ImmutableMap.of(
//                "from", "1",
//                "to", "10",
//                "delay", "0.4",
//                "topic", "any")))
//            .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
//        .apply(ParDo.of(new KafkaSourceConsumerFn<Integer>(
//            CounterSourceConnector.class, record -> (Integer) record.value())))
//        .setCoder(VarIntCoder.of());
//
//    PAssert.that(counts).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9 ,10);
//    p.run().waitUntilFinish();
//  }

  public static class SDFDatabaseHistory extends AbstractDatabaseHistory {

    private List<byte[]> history;

    public SDFDatabaseHistory() {
      this.history = new ArrayList<byte[]>();
    }

    @Override
    public void start() {
      super.start();
      System.out.println("STARTING THE DATABASE HISTORY! - trackers: "
          + KafkaSourceConsumerFn.restrictionTrackers.toString()
          + " - config: " + config.asMap().toString());
      String instanceId = config.asMap().getOrDefault(KafkaSourceConsumerFn.BEAM_INSTANCE_PROPERTY, null);
      // TODO(pabloem): Figure out how to link these. For now, we'll just link directly.
      RestrictionTracker<OffsetHolder, ?> tracker = KafkaSourceConsumerFn.restrictionTrackers.get(
          // JUST GETTING THE FIRST KEY. This will not work in the future.
          KafkaSourceConsumerFn.restrictionTrackers.keySet().iterator().next());
      this.history = (List<byte[]>) tracker.currentRestriction().history;
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
      System.out.println("Adding history! " + record.toString());
      history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
      System.out.println("Trying to recover!");
      try {
        for (byte[] record : history) {
          Document doc = DocumentReader.defaultReader().read(record);
          consumer.accept(new HistoryRecord(doc));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean exists() {
      return history != null && !history.isEmpty();
    }

    @Override
    public boolean storageExists() {
      return history != null && !history.isEmpty();
    }
  }

//  @Test
//  public void testDebeziumConnector() {
//    Pipeline p = Pipeline.create();
//    PCollection<String> counts = p.apply(
//        Create
//            .of(Lists.newArrayList(
//                (Map<String, String>) ImmutableMap.<String, String>builder()
//                  .put("connector.class", "io.debezium.connector.mysql.MySqlConnector")
//                  .put("database.hostname", "127.0.0.1")
//                  .put("database.port", "3306")
//                  .put("database.user", "debezium")
//                  .put("database.password", "dbz")
//                  .put("database.server.id", "184054")
//                  .put("database.server.name", "dbserver1")
//                  .put("database.include.list", "inventory")
//                  .put("database.history", SDFDatabaseHistory.class.getName())
//                  .put("include.schema.changes", "false")
//        .build()))
//            .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
//        .apply(ParDo.of(new KafkaSourceConsumerFn<String>(MySqlConnector.class, record -> {
//          System.out.println("GOT RECORD - " + record.toString());
//          return record.toString();
//        })))
//        .setCoder(StringUtf8Coder.of());
//
//    PAssert.that(counts).containsInAnyOrder();
//    p.run().waitUntilFinish();
//  }
  
  @Test
  public void testWordCountExample1() {
    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.read().from("file:///Users/osvaldo.salinas/Documents/TextFile.txt"))
            .apply("ExtractWords", FlatMapElements
                    .into(TypeDescriptors.strings())
                    .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
            .apply(Filter.by((String word) -> !word.isEmpty()))
            .apply(Count.<String>perElement())
            .apply("FormatResults", MapElements
                    .into(TypeDescriptors.strings())
                    .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
            .apply(TextIO.write().to("wordcounts"));
    System.out.println("Hi!");
    p.run().waitUntilFinish();

  }
  
  @Test
  public void testDebeziumIO() {
	  PipelineOptions options = PipelineOptionsFactory.create();
	  Pipeline p = Pipeline.create(options);
	  p.apply(
			  DebeziumIO.<String>read().
			  		withConnectorConfiguration(
						DebeziumIO.ConnectorConfiguration.create()
							.withUsername("debezium")
							.withPassword("dbz")
							.withConnectorClass(MySqlConnector.class)
							.withHostName("127.0.0.1")
							.withPort("3306")
							.withConnectionProperty("database.server.id", "184054")
							.withConnectionProperty("database.server.name", "dbserver1")
							.withConnectionProperty("database.include.list", "inventory")
							.withConnectionProperty("database.history", SDFDatabaseHistory.class.getName())
							.withConnectionProperty("include.schema.changes", "false")
              ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
      ).setCoder(StringUtf8Coder.of());
	  //.apply(TextIO.write().to("test"));

	  p.run().waitUntilFinish();
  }
  
    @Test
    public void testDebeziumIOPostgreSql() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                withConnectorConfiguration(
                        DebeziumIO.ConnectorConfiguration.create()
                                .withUsername("postgres")
                                .withPassword("debezium")
                                .withConnectorClass(PostgresConnector.class)
                                .withHostName("127.0.0.1")
                                .withPort("5000")
                                .withConnectionProperty("database.dbname", "postgres")
                                .withConnectionProperty("database.server.name", "dbserver2")
                                .withConnectionProperty("schema.include.list", "inventory")
                                .withConnectionProperty("slot.name", "dbzslot2")
                                .withConnectionProperty("database.history", SDFDatabaseHistory.class.getName())
                                .withConnectionProperty("include.schema.changes", "false")
                ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        ).setCoder(StringUtf8Coder.of());

        p.run().waitUntilFinish();
    }

    @Test
    public void testDebeziumIOSqlSever() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.apply(DebeziumIO.<String>read().
                        withConnectorConfiguration(
                                DebeziumIO.ConnectorConfiguration.create()
                                        .withUsername("sa")
                                        .withPassword("Password!")
                                        .withConnectorClass(SqlServerConnector.class)
                                        .withHostName("127.0.0.1")
                                        .withPort("1433")
                                        .withConnectionProperty("database.dbname", "testDB")
                                        .withConnectionProperty("database.server.name", "server1")
                                        .withConnectionProperty("table.include.list", "dbo.customers")
                                        .withConnectionProperty("database.history", SDFDatabaseHistory.class.getName())
                                        .withConnectionProperty("include.schema.changes", "false")
                        ).withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
        ).setCoder(StringUtf8Coder.of());

        p.run().waitUntilFinish();
    }
  
}
