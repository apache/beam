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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.alibaba.fastjson.JSON;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** This is an integration test for KafkaCSVTable. */
public class KafkaCSVTableIT {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

  private KafkaOptions kafkaOptions;

  private static final Schema TEST_TABLE_SCHEMA =
      Schema.builder()
          .addNullableField("order_id", Schema.FieldType.INT32)
          .addNullableField("member_id", Schema.FieldType.INT32)
          .addNullableField("item_name", Schema.FieldType.INT32)
          .build();

  @Before
  public void setUp() {
    kafkaOptions = pipeline.getOptions().as(KafkaOptions.class);
    kafkaOptions.setKafkaTopic("topic");
    kafkaOptions.setKafkaBootstrapServerAddress(kafka.getBootstrapServers());
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testFake2() throws BeamKafkaTable.NoEstimationException {
    Table table =
        Table.builder()
            .name("kafka_table")
            .comment("kafka" + " table")
            .location("")
            .schema(
                Stream.of(
                        Schema.Field.nullable("order_id", INT32),
                        Schema.Field.nullable("member_id", INT32),
                        Schema.Field.nullable("item_name", INT32))
                    .collect(toSchema()))
            .type("kafka")
            .properties(JSON.parseObject(getKafkaPropertiesString()))
            .build();
    BeamKafkaTable kafkaTable = (BeamKafkaTable) new KafkaTableProvider().buildBeamSqlTable(table);
    produceSomeRecordsWithDelay(100, 20);
    double rate1 = kafkaTable.computeRate(20);
    produceSomeRecordsWithDelay(100, 10);
    double rate2 = kafkaTable.computeRate(20);
    Assert.assertTrue(rate2 > rate1);
  }

  private String getKafkaPropertiesString() {
    return "{ \"bootstrap.servers\" : \""
        + kafkaOptions.getKafkaBootstrapServerAddress()
        + "\",\"topics\":[\""
        + kafkaOptions.getKafkaTopic()
        + "\"] }";
  }

  static final transient Map<Long, Boolean> FLAG = new ConcurrentHashMap<>();

  @Test
  public void testFake() throws InterruptedException {
    pipeline.getOptions().as(DirectOptions.class).setBlockOnRun(false);
    String createTableString =
        "CREATE EXTERNAL TABLE kafka_table(\n"
            + "order_id INTEGER, \n"
            + "member_id INTEGER, \n"
            + "item_name INTEGER \n"
            + ") \n"
            + "TYPE 'kafka' \n"
            + "LOCATION '"
            + "'\n"
            + "TBLPROPERTIES '"
            + getKafkaPropertiesString()
            + "'";
    TableProvider tb = new KafkaTableProvider();
    BeamSqlEnv env = BeamSqlEnv.inMemory(tb);

    env.executeDdl(createTableString);

    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM kafka_table"));

    queryOutput
        .apply(ParDo.of(new FakeKvPair()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(TEST_TABLE_SCHEMA)))
        .apply(
            "waitForSuccess",
            ParDo.of(
                new StreamAssertEqual(
                    ImmutableSet.of(
                        row(TEST_TABLE_SCHEMA, 0, 1, 0),
                        row(TEST_TABLE_SCHEMA, 1, 2, 1),
                        row(TEST_TABLE_SCHEMA, 2, 3, 2)))));
    queryOutput.apply(logRecords(""));
    pipeline.run();
    TimeUnit.MILLISECONDS.sleep(3000);
    produceSomeRecords(3);

    for (int i = 0; i < 200; i++) {
      if (FLAG.getOrDefault(pipeline.getOptions().getOptionsId(), false)) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(60);
    }
    Assert.fail();
  }

  private static MapElements<Row, Void> logRecords(String suffix) {
    return MapElements.via(
        new SimpleFunction<Row, Void>() {
          @Override
          public @Nullable Void apply(Row input) {
            System.out.println(input.getValues() + suffix);
            return null;
          }
        });
  }

  /** This is made because DoFn with states should get KV as input. */
  public static class FakeKvPair extends DoFn<Row, KV<String, Row>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(KV.of("fake_key", c.element()));
    }
  }

  /** This DoFn will set a flag if all the elements are seen. */
  public static class StreamAssertEqual extends DoFn<KV<String, Row>, Void> {
    private final Set<Row> expected;

    StreamAssertEqual(Set<Row> expected) {
      super();
      this.expected = expected;
    }

    @StateId("seenValues")
    private final StateSpec<BagState<Row>> seenRows =
        StateSpecs.bag(RowCoder.of(TEST_TABLE_SCHEMA));

    @StateId("count")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

    @ProcessElement
    public void process(
        ProcessContext context,
        @StateId("seenValues") BagState<Row> seenValues,
        @StateId("count") ValueState<Integer> countState) {
      // I don't think doing this will be safe in parallel
      int count = MoreObjects.firstNonNull(countState.read(), 0);
      count = count + 1;
      countState.write(count);
      seenValues.add(context.element().getValue());

      if (count >= expected.size()) {
        if (StreamSupport.stream(seenValues.read().spliterator(), false)
            .collect(Collectors.toSet())
            .containsAll(expected)) {
          System.out.println("in second if");
          FLAG.put(context.getPipelineOptions().getOptionsId(), true);
        }
      }
    }
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void produceSomeRecords(int num) {
    Producer<String, String> producer = new KafkaProducer<String, String>(producerProps());
    String topicName = kafkaOptions.getKafkaTopic();
    for (int i = 0; i < num; i++) {
      producer.send(
          new ProducerRecord<String, String>(
              topicName, "k" + i, i + "," + ((i % 3) + 1) + "," + i));
    }
    producer.flush();
    producer.close();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void produceSomeRecordsWithDelay(int num, int delayMilis) {
    Producer<String, String> producer = new KafkaProducer<String, String>(producerProps());
    String topicName = pipeline.getOptions().as(KafkaOptions.class).getKafkaTopic();
    for (int i = 0; i < num; i++) {
      producer.send(
          new ProducerRecord<String, String>(
              topicName, "k" + i, i + "," + ((i % 3) + 1) + "," + i));
      try {
        TimeUnit.MILLISECONDS.sleep(delayMilis);
      } catch (InterruptedException e) {
        throw new RuntimeException("Could not wait for producing", e);
      }
    }
    producer.flush();
    producer.close();
  }

  private Properties producerProps() {
    KafkaOptions options = pipeline.getOptions().as(KafkaOptions.class);
    Properties props = new Properties();
    props.put("bootstrap.servers", options.getKafkaBootstrapServerAddress());
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("buffer.memory", 33554432);
    props.put("acks", "all");
    props.put("request.required.acks", "1");
    props.put("retries", 0);
    props.put("linger.ms", 1);
    return props;
  }

  /** Pipeline options specific for this test. */
  public interface KafkaOptions extends PipelineOptions {

    @Description("Kafka server address")
    @Validation.Required
    @Default.String("localhost:9092")
    String getKafkaBootstrapServerAddress();

    void setKafkaBootstrapServerAddress(String address);

    @Description("Kafka topic")
    @Validation.Required
    @Default.String("test")
    String getKafkaTopic();

    void setKafkaTopic(String topic);
  }
}
