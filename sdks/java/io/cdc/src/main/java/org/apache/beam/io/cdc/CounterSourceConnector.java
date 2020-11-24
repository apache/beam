package org.apache.beam.io.cdc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

public class CounterSourceConnector extends SourceConnector {

  public static class CounterSourceConnectorConfig extends AbstractConfig {
    final Map<String, String> props;

    CounterSourceConnectorConfig(Map<String, String> props) {
      super(configDef(), props);
      this.props = props;
    }

    protected static ConfigDef configDef() {
      return new ConfigDef()
          .define("from",
              ConfigDef.Type.INT,
              ConfigDef.Importance.HIGH,
              "Number to start from")
          .define("to",
              ConfigDef.Type.INT,
              ConfigDef.Importance.HIGH,
              "Number to go to")
          .define("delay",
              ConfigDef.Type.DOUBLE,
              ConfigDef.Importance.HIGH,
              "Time between each event")
          .define("topic",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Name of Kafka topic to produce to");
    }
  }

  @Nullable
  private CounterSourceConnectorConfig connectorConfig = null;

  @Override
  public void start(Map<String, String> props) {
    this.connectorConfig = new CounterSourceConnectorConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CounterTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return connectorConfig == null || connectorConfig.props == null
        ? Collections.emptyList()
        : Collections.singletonList(ImmutableMap.of(
            "from", connectorConfig.props.get("from"),
          "to", connectorConfig.props.get("to"),
          "delay", connectorConfig.props.get("delay"),
          "topic", connectorConfig.props.get("topic")
    ));
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define("from",
            ConfigDef.Type.INT,
            ConfigDef.Importance.HIGH,
            "Number to start from")
        .define("to",
            ConfigDef.Type.INT,
            ConfigDef.Importance.HIGH,
            "Number to go to")
        .define("delay",
            ConfigDef.Type.DOUBLE,
            ConfigDef.Importance.HIGH,
            "Time between each event")
        .define("topic",
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Name of Kafka topic to produce to");
  }

  @Override
  public String version() {
    return "ONE";
  }

  public static class CounterTask extends SourceTask {

    // NOTES
    // A task can write to multiple topics.

    private String topic = "";
    private Integer from = 0;
    private Integer to = 0;
    private Double delay = 0.0;
    private Long start = System.currentTimeMillis();
    private Integer last = 0;
    private Object lastOffset = null;

    private static final String PARTITION_FIELD = "mod";
    private static final Integer PARTITION_NAME = 1;

    @Override
    public String version() {
      return "ONE";
    }

    @Override
    public void initialize(SourceTaskContext context) {
      super.initialize(context);
      // Get the offset for our corresponding partiton
      Map<String, Object> offset = context.offsetStorageReader()
          .offset(Collections.singletonMap(PARTITION_FIELD, PARTITION_NAME));
      System.out.println("Initializing with offset " + String.format("%s", offset));
      this.last = offset == null ? 0 : ((Long) offset.getOrDefault("last", 0)).intValue();
      this.start = offset == null ? System.currentTimeMillis() : (Long) offset.get("start");
      this.lastOffset = offset;
    }

    @Override
    public void start(Map<String, String> props) {
      this.topic = props.getOrDefault("topic", "");
      this.from = Integer.parseInt(props.getOrDefault("from", "0"));
      this.to = Integer.parseInt(props.getOrDefault("to", "0"));
      this.delay = Double.parseDouble(props.getOrDefault("delay", "0"));

      // Only do this if we have not consumed an offset
      if (lastOffset != null) return;
      this.start = props.containsKey("start") ? Long.parseLong(props.get("start")) : System.currentTimeMillis();
      this.last = from - 1;
    }

    @Override
    @Nullable
    public List<SourceRecord> poll() throws InterruptedException {
      if (last.equals(to)) {
        return null;
      }
      List<SourceRecord> result = new ArrayList<>();
      Long callTime = System.currentTimeMillis();
      Long secondsSinceStart = (callTime - this.start) / 1000;

      Long elementsToOutput = Math.round(Math.floor(secondsSinceStart / delay));
      // qq. How to deal with restarts, and with second-calls?
      while(last < to) {
        last = last + 1;
        Map<String, Integer> sourcePartition = Collections.singletonMap("mod", 1);
        Map<String, Long> sourceOffset = ImmutableMap.of("last", last.longValue(), "start", start);
        result.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.INT64_SCHEMA, last));
        if (result.size() >= elementsToOutput) {
          break;
        }
      }
      return result;
    }

    @Override
    public void stop() {

    }
  }
}