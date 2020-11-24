package org.apache.beam.io.cdc;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.beam.io.cdc.KafkaSourceConsumerFn.OffsetHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CounterSourceConnectorTest {

  @Test
  public void testQuickCount() {
    Pipeline p = Pipeline.create();
    PCollection<Integer> counts = p.apply(
        Create
            .of(Lists.newArrayList((Map<String, String>) ImmutableMap.of(
                "from", "1",
                "to", "10",
                "delay", "0.4",
                "topic", "any")))
            .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply(ParDo.of(new KafkaSourceConsumerFn<Integer>(
            CounterSourceConnector.class, record -> (Integer) record.value())))
        .setCoder(VarIntCoder.of());

    PAssert.that(counts).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9 ,10);
    p.run().waitUntilFinish();
  }

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

  @Test
  public void testDebeziumConnector() {
    Pipeline p = Pipeline.create();
    PCollection<String> counts = p.apply(
        Create
            .of(Lists.newArrayList(
                (Map<String, String>) ImmutableMap.<String, String>builder()
                  .put("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                  .put("database.hostname", "127.0.0.1")
                  .put("database.port", "3306")
                  .put("database.user", "debezium")
                  .put("database.password", "dbz")
                  .put("database.server.id", "184054")
                  .put("database.server.name", "dbserver1")
                  .put("database.include.list", "inventory")
                  .put("database.history", SDFDatabaseHistory.class.getName())
                  .put("include.schema.changes", "false")
        .build()))
            .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply(ParDo.of(new KafkaSourceConsumerFn<String>(MySqlConnector.class, record -> {
          System.out.println("GOT RECORD - " + record.toString());
          return record.toString();
        })))
        .setCoder(StringUtf8Coder.of());

    PAssert.that(counts).containsInAnyOrder();
    p.run().waitUntilFinish();
  }
}
