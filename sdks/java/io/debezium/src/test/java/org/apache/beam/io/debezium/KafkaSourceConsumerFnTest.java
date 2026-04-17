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
package org.apache.beam.io.debezium;

import com.google.common.testing.EqualsTester;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.io.debezium.KafkaSourceConsumerFn.OffsetHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestOutputReceiver;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaSourceConsumerFnTest implements Serializable {

  static <T> DebeziumIO.Read<T> getSpec(SourceRecordMapper<T> fn, Integer maxRecords) {
    DebeziumIO.Read<T> transform = DebeziumIO.<T>read().withFormatFunction(fn);
    if (maxRecords > 0) {
      transform = transform.withMaxNumberOfRecords(maxRecords);
    }
    return transform;
  }

  @After
  public void cleanUp() {
    CounterTask.resetCountTask();
  }

  @Test
  public void testKafkaSourceConsumerFn() {
    Map<String, String> config =
        ImmutableMap.of(
            "from", "1",
            "to", "10",
            "delay", "0.4",
            "topic", "any");

    Pipeline pipeline = Pipeline.create();

    PCollection<Integer> counts =
        pipeline
            .apply(
                Create.of(Lists.newArrayList(config))
                    .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                ParDo.of(
                    new KafkaSourceConsumerFn<>(
                        CounterSourceConnector.class,
                        getSpec(
                            sourceRecord ->
                                ((Struct) sourceRecord.value()).getInt64("value").intValue(),
                            10))))
            .setCoder(VarIntCoder.of());

    PAssert.that(counts).containsInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testStoppableKafkaSourceConsumerFn() {
    Map<String, String> config =
        ImmutableMap.of(
            "from", "1",
            "to", "3",
            "delay", "0.2",
            "topic", "any");

    Pipeline pipeline = Pipeline.create();

    pipeline
        .apply(
            Create.of(Lists.newArrayList(config))
                .withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply(
            ParDo.of(
                new KafkaSourceConsumerFn<>(
                    CounterSourceConnector.class,
                    getSpec(
                        sourceRecord ->
                            ((Struct) sourceRecord.value()).getInt64("value").intValue(),
                        1))))
        .setCoder(VarIntCoder.of());

    pipeline.run().waitUntilFinish();
    Assert.assertEquals(1, CounterTask.getCountTasks());
  }

  @Test
  public void testKafkaOffsetHolderEquality() {
    EqualsTester tester = new EqualsTester();

    HashMap<String, Integer> map = new HashMap<>();
    map.put("a", 1);
    map.put("b", 2);
    ArrayList<byte[]> list = new ArrayList<>();
    list.add("abc".getBytes(StandardCharsets.US_ASCII));
    list.add(new byte[0]);
    tester.addEqualityGroup(
        new OffsetHolder(
            ImmutableMap.of("a", 1, "b", 2),
            ImmutableList.of("abc".getBytes(StandardCharsets.US_ASCII), new byte[0]),
            1,
            null,
            -1L),
        new OffsetHolder(map, list, 1, null, -1L),
        new OffsetHolder(map, list, 1, null, -1L),
        new OffsetHolder(map, list, 1));
    tester.addEqualityGroup(new OffsetHolder(null, null, null, null, null));
    tester.addEqualityGroup(
        new OffsetHolder(
            ImmutableMap.of("a", 1),
            ImmutableList.of("abc".getBytes(StandardCharsets.US_ASCII)),
            1));
    tester.addEqualityGroup(
        new OffsetHolder(
            ImmutableMap.of("a", 1),
            ImmutableList.of("abc".getBytes(StandardCharsets.US_ASCII)),
            2));
    tester.addEqualityGroup(
        new OffsetHolder(
            ImmutableMap.of("a", 1),
            ImmutableList.of("abc".getBytes(StandardCharsets.US_ASCII)),
            1,
            2,
            null));
    tester.addEqualityGroup(
        new OffsetHolder(
            ImmutableMap.of("a", 1),
            ImmutableList.of("abc".getBytes(StandardCharsets.US_ASCII)),
            1,
            3,
            null));
    tester.testEquals();
  }

  @Test(timeout = 2000)
  public void testMaxTimeToRun() throws IOException {
    KafkaSourceConsumerFn<Integer> kafkaSourceConsumerFn =
        new KafkaSourceConsumerFn<>(
            CounterSourceConnector.class,
            KafkaSourceConsumerFnTest.getSpec(
                    sourceRecord -> ((Struct) sourceRecord.value()).getInt64("value").intValue(), 0)
                .withPollingTimeout(100L)
                .withMaxTimeToRun(500L)); // Run for 0.5 s
    kafkaSourceConsumerFn.setup();
    OffsetHolder initialRestriction = kafkaSourceConsumerFn.getInitialRestriction(null);
    RestrictionTracker<OffsetHolder, Map<String, Object>> tracker =
        kafkaSourceConsumerFn.newTracker(initialRestriction);
    Map<String, String> config =
        ImmutableMap.of("from", "1", "delay", "0.4", "sleep", "1", "topic", "any");
    TestOutputReceiver<Integer> receiver = new TestOutputReceiver<>();
    while (true) {
      DoFn.ProcessContinuation continuation =
          kafkaSourceConsumerFn.process(config, tracker, receiver);
      if (continuation == DoFn.ProcessContinuation.stop()) {
        break;
      }
    }
    // Check results are in order
    ListIterator<Integer> it = receiver.getOutputs().listIterator();
    while (it.hasNext()) {
      Assert.assertEquals(it.nextIndex(), it.next() - 1);
    }
  }
}

class CounterSourceConnector extends SourceConnector {
  public static class CounterSourceConnectorConfig extends AbstractConfig {
    final Map<String, String> props;

    CounterSourceConnectorConfig(Map<String, String> props) {
      super(configDef(), props);
      this.props = props;
    }

    protected static ConfigDef configDef() {
      return new ConfigDef()
          .define("from", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Number to start from")
          .define("to", ConfigDef.Type.INT, -1, ConfigDef.Importance.HIGH, "Number to go to")
          .define(
              "delay", ConfigDef.Type.DOUBLE, ConfigDef.Importance.HIGH, "Time between each event")
          .define(
              "sleep",
              ConfigDef.Type.INT,
              0,
              ConfigDef.Importance.MEDIUM,
              "Millis to sleep in each poll")
          .define(
              "topic",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "Name of Kafka topic to produce to");
    }
  }

  @Nullable private CounterSourceConnectorConfig connectorConfig;

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
    if (this.connectorConfig == null || this.connectorConfig.props == null) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        ImmutableMap.of(
            "from", this.connectorConfig.props.get("from"),
            "to", this.connectorConfig.props.getOrDefault("to", "-1"),
            "delay", this.connectorConfig.props.get("delay"),
            "sleep", this.connectorConfig.props.getOrDefault("sleep", "0"),
            "topic", this.connectorConfig.props.get("topic")));
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return CounterSourceConnectorConfig.configDef();
  }

  @Override
  public String version() {
    return "ONE";
  }
}

@NotThreadSafe
class CounterTask extends SourceTask {
  private static int countStopTasks = 0;
  private String topic = "";
  private Integer from = 0;
  private Integer to = 0;
  private Integer sleep = 0;
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

    Map<String, Object> offset =
        context
            .offsetStorageReader()
            .offset(Collections.singletonMap(PARTITION_FIELD, PARTITION_NAME));

    if (offset == null) {
      this.start = System.currentTimeMillis();
      this.last = 0;
    } else {
      this.start = (Long) offset.get("start");
      this.last = ((Long) offset.getOrDefault("last", 0)).intValue();
    }
    this.lastOffset = offset;
  }

  @Override
  public void start(Map<String, String> props) {
    this.topic = props.getOrDefault("topic", "");
    this.from = Integer.parseInt(props.getOrDefault("from", "0"));
    this.to = Integer.parseInt(props.getOrDefault("to", "-1"));
    this.delay = Double.parseDouble(props.getOrDefault("delay", "0"));
    this.sleep = Integer.parseInt(props.getOrDefault("sleep", "0"));

    if (this.lastOffset != null) {
      return;
    }

    this.start =
        props.containsKey("start")
            ? Long.parseLong(props.get("start"))
            : System.currentTimeMillis();
    this.last = this.from - 1;
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    if (this.last.equals(to)) {
      return null;
    }
    Schema recordSchema =
        SchemaBuilder.struct()
            .field("value", Schema.INT64_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();

    List<SourceRecord> records = new ArrayList<>();
    Long callTime = System.currentTimeMillis();
    Long secondsSinceStart = (callTime - this.start) / 1000;
    Long recordsToOutput = Math.round(Math.floor(secondsSinceStart / this.delay));

    while (this.to == -1 || this.last < this.to) {
      this.last = this.last + 1;
      Map<String, Integer> sourcePartition = Collections.singletonMap(PARTITION_FIELD, 1);
      Map<String, Long> sourceOffset =
          ImmutableMap.of("last", this.last.longValue(), "start", this.start);

      records.add(
          new SourceRecord(
              sourcePartition,
              sourceOffset,
              this.topic,
              recordSchema,
              new Struct(recordSchema)
                  .put("value", this.last.longValue())
                  .put("ts_ms", this.last.longValue())));

      if (records.size() >= recordsToOutput) {
        break;
      }
    }
    if (this.sleep > 0) {
      Thread.sleep(this.sleep);
    }
    return records;
  }

  @Override
  public void stop() {
    CounterTask.countStopTasks++;
  }

  public static int getCountTasks() {
    return CounterTask.countStopTasks;
  }

  public static void resetCountTask() {
    CounterTask.countStopTasks = 0;
  }
}
