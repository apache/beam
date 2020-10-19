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

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.ReuseSparkContextRule;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.runners.spark.UsesCheckpointRecovery;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.translation.streaming.utils.EmbeddedKafkaCluster;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.serialization.InstantDeserializer;
import org.apache.beam.sdk.io.kafka.serialization.InstantSerializer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests DStream recovery from checkpoint.
 *
 * <p>Runs the pipeline reading from a Kafka backlog with a WM function that will move to infinity
 * on a EOF signal. After resuming from checkpoint, a single output (guaranteed by the WM) is
 * asserted, along with {@link Metrics} values that are expected to resume from previous count and a
 * side-input that is expected to recover as well.
 */
@RunWith(JUnit4.class)
public class ResumeFromCheckpointStreamingTest implements Serializable {
  private static final EmbeddedKafkaCluster.EmbeddedZookeeper EMBEDDED_ZOOKEEPER =
      new EmbeddedKafkaCluster.EmbeddedZookeeper();
  private static final EmbeddedKafkaCluster EMBEDDED_KAFKA_CLUSTER =
      new EmbeddedKafkaCluster(EMBEDDED_ZOOKEEPER.getConnection(), new Properties());
  private static final String TOPIC = "kafka_beam_test_topic";

  private transient TemporaryFolder temporaryFolder;

  @Rule public final transient ReuseSparkContextRule noContextReuse = ReuseSparkContextRule.no();

  @BeforeClass
  public static void setup() throws IOException {
    EMBEDDED_ZOOKEEPER.startup();
    EMBEDDED_KAFKA_CLUSTER.startup();
  }

  @Before
  public void init() {
    temporaryFolder = new TemporaryFolder();
    try {
      temporaryFolder.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private static void produce(Map<String, Instant> messages) {
    Properties producerProps = new Properties();
    producerProps.putAll(EMBEDDED_KAFKA_CLUSTER.getProps());
    producerProps.put("request.required.acks", 1);
    producerProps.put("bootstrap.servers", EMBEDDED_KAFKA_CLUSTER.getBrokerList());
    Serializer<String> stringSerializer = new StringSerializer();
    Serializer<Instant> instantSerializer = new InstantSerializer();

    try (KafkaProducer<String, Instant> kafkaProducer =
        new KafkaProducer(producerProps, stringSerializer, instantSerializer)) {
      for (Map.Entry<String, Instant> en : messages.entrySet()) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC, en.getKey(), en.getValue()));
      }
    }
  }

  @Test
  @Category(UsesCheckpointRecovery.class)
  public void testWithResume() throws Exception {
    // write to Kafka
    produce(
        ImmutableMap.of(
            "k1", new Instant(100),
            "k2", new Instant(200),
            "k3", new Instant(300),
            "k4", new Instant(400)));

    MetricsFilter metricsFilter =
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(ResumeFromCheckpointStreamingTest.class))
            .build();

    // first run should expect EOT matching the last injected element.
    SparkPipelineResult res = run(Optional.of(new Instant(400)), 0);

    assertThat(
        res.metrics().queryMetrics(metricsFilter).getCounters(),
        hasItem(
            attemptedMetricsResult(
                ResumeFromCheckpointStreamingTest.class.getName(),
                "allMessages",
                "EOFShallNotPassFn",
                4L)));
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getCounters(),
        hasItem(
            attemptedMetricsResult(
                ResumeFromCheckpointStreamingTest.class.getName(),
                "processedMessages",
                "EOFShallNotPassFn",
                4L)));

    // --- between executions:

    // - clear state.
    clean();

    // - write a bit more.
    produce(
        ImmutableMap.of(
            "k5", new Instant(499),
            "EOF", new Instant(500) // to be dropped from [0, 500).
            ));

    // recovery should resume from last read offset, and read the second batch of input.
    res = runAgain(1);
    // assertions 2:
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getCounters(),
        hasItem(
            attemptedMetricsResult(
                ResumeFromCheckpointStreamingTest.class.getName(),
                "processedMessages",
                "EOFShallNotPassFn",
                5L)));
    assertThat(
        res.metrics().queryMetrics(metricsFilter).getCounters(),
        hasItem(
            attemptedMetricsResult(
                ResumeFromCheckpointStreamingTest.class.getName(),
                "allMessages",
                "EOFShallNotPassFn",
                6L)));
    long successAssertions = 0;
    Iterable<MetricResult<Long>> counterResults =
        res.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            PAssertWithoutFlatten.class, PAssert.SUCCESS_COUNTER))
                    .build())
            .getCounters();
    for (MetricResult<Long> counter : counterResults) {
      if (counter.getAttempted() > 0) {
        successAssertions++;
      }
    }
    assertThat(
        String.format("Expected %d successful assertions, but found %d.", 1L, successAssertions),
        successAssertions,
        is(1L));
    // validate assertion didn't fail.
    long failedAssertions = 0;
    Iterable<MetricResult<Long>> failCounterResults =
        res.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            PAssertWithoutFlatten.class, PAssert.FAILURE_COUNTER))
                    .build())
            .getCounters();
    for (MetricResult<Long> counter : failCounterResults) {
      if (counter.getAttempted() > 0) {
        failedAssertions++;
      }
    }
    assertThat(
        String.format("Found %d failed assertions.", failedAssertions), failedAssertions, is(0L));
  }

  private SparkPipelineResult runAgain(int expectedAssertions) {
    // sleep before next run.
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    return run(Optional.absent(), expectedAssertions);
  }

  private SparkPipelineResult run(Optional<Instant> stopWatermarkOption, int expectedAssertions) {
    KafkaIO.Read<String, Instant> read =
        KafkaIO.<String, Instant>read()
            .withBootstrapServers(EMBEDDED_KAFKA_CLUSTER.getBrokerList())
            .withTopics(Collections.singletonList(TOPIC))
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(InstantDeserializer.class)
            .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
            .withTimestampFn(KV::getValue)
            .withWatermarkFn(
                kv -> {
                  // at EOF move WM to infinity.
                  String key = kv.getKey();
                  Instant instant = kv.getValue();
                  return "EOF".equals(key) ? BoundedWindow.TIMESTAMP_MAX_VALUE : instant;
                });

    TestSparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setSparkMaster("local[*]");
    options.setCheckpointDurationMillis(options.getBatchIntervalMillis());
    options.setExpectedAssertions(expectedAssertions);
    options.setRunner(TestSparkRunner.class);
    options.setEnableSparkMetricSinks(false);
    options.setForceStreaming(true);
    options.setCheckpointDir(temporaryFolder.getRoot().getPath());
    // timeout is per execution so it can be injected by the caller.
    if (stopWatermarkOption.isPresent()) {
      options.setStopPipelineWatermark(stopWatermarkOption.get().getMillis());
    }

    Pipeline p = Pipeline.create(options);

    PCollection<String> expectedCol =
        p.apply(Create.of(ImmutableList.of("side1", "side2")).withCoder(StringUtf8Coder.of()));
    PCollectionView<List<String>> view = expectedCol.apply(View.asList());

    PCollection<KV<String, Instant>> kafkaStream = p.apply(read.withoutMetadata());

    PCollection<Iterable<String>> grouped =
        kafkaStream
            .apply(Keys.create())
            .apply("EOFShallNotPassFn", ParDo.of(new EOFShallNotPassFn(view)).withSideInputs(view))
            .apply(
                Window.<String>into(FixedWindows.of(Duration.millis(500)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create());

    grouped.apply(new PAssertWithoutFlatten<>("k1", "k2", "k3", "k4", "k5"));

    return (SparkPipelineResult) p.run();
  }

  @After
  public void clean() {
    AggregatorsAccumulator.clear();
    MetricsAccumulator.clear();
    GlobalWatermarkHolder.clear();
    MicrobatchSource.clearCache();
  }

  @AfterClass
  public static void tearDown() {
    EMBEDDED_KAFKA_CLUSTER.shutdown();
    EMBEDDED_ZOOKEEPER.shutdown();
  }

  /** A pass-through fn that prevents EOF event from passing. */
  private static class EOFShallNotPassFn extends DoFn<String, String> {
    final PCollectionView<List<String>> view;
    private final Counter aggregator =
        Metrics.counter(ResumeFromCheckpointStreamingTest.class, "processedMessages");
    final Counter counter = Metrics.counter(ResumeFromCheckpointStreamingTest.class, "allMessages");

    private EOFShallNotPassFn(PCollectionView<List<String>> view) {
      this.view = view;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      String element = c.element();
      // assert that side input is passed correctly before/after resuming from checkpoint.
      assertThat(c.sideInput(view), containsInAnyOrder("side1", "side2"));
      counter.inc();
      if (!"EOF".equals(element)) {
        aggregator.inc();
        c.output(c.element());
      }
    }
  }

  /**
   * A custom PAssert that avoids using {@link org.apache.beam.sdk.transforms.Flatten} until
   * BEAM-1444 is resolved.
   */
  private static class PAssertWithoutFlatten<T>
      extends PTransform<PCollection<Iterable<T>>, PDone> {
    private final T[] expected;

    @SafeVarargs
    private PAssertWithoutFlatten(T... expected) {
      this.expected = expected;
    }

    @Override
    public PDone expand(PCollection<Iterable<T>> input) {
      input.apply(ParDo.of(new AssertDoFn<>(expected)));
      return PDone.in(input.getPipeline());
    }

    private static class AssertDoFn<T> extends DoFn<Iterable<T>, Void> {
      private final Counter success =
          Metrics.counter(PAssertWithoutFlatten.class, PAssert.SUCCESS_COUNTER);
      private final Counter failure =
          Metrics.counter(PAssertWithoutFlatten.class, PAssert.FAILURE_COUNTER);
      private final T[] expected;

      AssertDoFn(T[] expected) {
        this.expected = expected;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        try {
          assertThat(c.element(), containsInAnyOrder(expected));
          success.inc();
        } catch (Throwable t) {
          failure.inc();
          throw t;
        }
      }
    }
  }
}
