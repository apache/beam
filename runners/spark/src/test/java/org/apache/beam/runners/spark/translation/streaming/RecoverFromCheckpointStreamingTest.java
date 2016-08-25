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
package org.apache.beam.runners.spark.translation.streaming;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import kafka.serializer.StringDecoder;

import org.apache.beam.runners.spark.BroadcastSideInputs;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.io.KafkaIO;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.streaming.Durations;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test DStream recovery from checkpoint.
 * Check Aggregators and SideInputs which rely on Accumulators and Broadcast (respectively).
 */
public class RecoverFromCheckpointStreamingTest {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());
  private static final String TOPIC = "kafka_dataflow_test_topic";
  private static final Map<String, String> KAFKA_MESSAGES = ImmutableMap.of(
      "k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4"
  );
  private static final String IGNORE = "4";
  private static final String[] EXPECTED = {"k1,v1", "k2,v2", "k3,v3", "k,v"};
  private static final long EXPECTED_4 = 4L;
  //TODO: once Accumulators can recover value, it should expect 8.
//  private static final long EXPECTED_8 = 8L;
  private static final long TEST_TIMEOUT_MSEC = 1000L;

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @BeforeClass
  public static void init() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();

    // write to Kafka
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("request.required.acks", 1);
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    try (@SuppressWarnings("unchecked") KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, stringSerializer)) {
      for (Map.Entry<String, String> en : KAFKA_MESSAGES.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
      }
      kafkaProducer.close();
    }
  }

  @Test
  public void testRun() throws Exception {
    // init the temp folder to be used by checkpointDir.
    checkpointParentDir.create();

    EvaluationResult res = run(checkpointParentDir.getRoot().getAbsolutePath());
    res.close();
    long emptyLines1 = res.getAggregatorValue("processedMessages", Long.class);
    assert emptyLines1 == EXPECTED_4 : String.format("Expected %d processed messages count but "
        + "found %d", EXPECTED_4, emptyLines1);

    // sleep before next run.
    Thread.sleep(100);

    res = runAgain(checkpointParentDir.getRoot().getAbsolutePath());
    res.close();
    long emptyLines2 = res.getAggregatorValue("processedMessages", Long.class);
    assert emptyLines2 == EXPECTED_4 : String.format("Expected %d processed messages count but "
        + "found %d", EXPECTED_4, emptyLines2);
  }

  private static EvaluationResult runAgain(String checkpointParentDir) {
    BroadcastSideInputs.clear();
    AccumulatorSingleton.clear();
    return run(checkpointParentDir);
  }

  private static EvaluationResult run(String checkpointParentDir) {
    SparkPipelineOptions options =
            PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    options.setSparkMaster("local[*]");
    options.setStreaming(true);
    options.setBatchIntervalMillis(Durations.seconds(1).milliseconds());
    options.setTimeout(TEST_TIMEOUT_MSEC); // run for one interval
    options.setCheckpointDir(checkpointParentDir + "/tmp/recover-from-checkpoint-streaming-test");
    Map<String, String> kafkaParams = ImmutableMap.of(
            "metadata.broker.list", EMBEDDED_KAFKA_CLUSTER.getBrokerList(),
            "auto.offset.reset", "smallest"
    );
    Pipeline p = Pipeline.create(options);
    PCollection<KV<String, String>> kafkaInput = p.apply(KafkaIO.Read.from(
        StringDecoder.class, StringDecoder.class, String.class, String.class,
            Collections.singleton(TOPIC), kafkaParams)).setCoder(KvCoder.of(StringUtf8Coder.of(),
                StringUtf8Coder.of()));
    PCollection<KV<String, String>> windowedWords = kafkaInput
        .apply(Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(1))));
    PCollectionView<String> sideInput = p.apply(Create.of(IGNORE)).apply(
        View.<String>asSingleton());
    PCollection<String> formattedKV = windowedWords.apply(ParDo.of(
        new FormatAsTextAndReplace(sideInput)).withSideInputs(sideInput));

    PAssertStreaming.assertContents(formattedKV, EXPECTED);

    return  (EvaluationResult) p.run();
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  private static class FormatAsTextAndReplace extends DoFn<KV<String, String>, String> {

    private final PCollectionView<String> ignore;

    private FormatAsTextAndReplace(PCollectionView<String> ignore) {
      this.ignore = ignore;
    }

    private final Aggregator<Long, Long> aggregator =
        createAggregator("processedMessages", new Sum.SumLongFn());

    @ProcessElement
    public void process(ProcessContext c) {
      aggregator.addValue(1L);
      String ignoreChar = c.sideInput(ignore);
      String formatted = c.element().getKey() + "," + c.element().getValue();
      c.output(formatted.replace(IGNORE, ""));
    }
  }

}
