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
package org.apache.beam.examples;

// beam-playground:
//   name: KafkaStreaming
//   description: Example of streaming data processing using Kafka
//   multifile: false
//   context_line: 186
//   never_run: true
//   always_run: true
//   categories:
//     - Filtering
//     - Windowing
//     - Streaming
//     - IO
//   complexity: MEDIUM
//   tags:
//     - strings
//     - pairs
//     - emulator

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class KafkaStreaming {

  // Kafka topic name
  private static final String TOPIC_NAME = "my-topic";

  // The deadline for processing late data
  private static final int ALLOWED_LATENESS_TIME = 1;

  // Delay time after the first element in window
  private static final int TIME_OUTPUT_AFTER_FIRST_ELEMENT = 10;

  // The time of the window in which the elements will be processed
  private static final int WINDOW_TIME = 30;

  // Number of game events to send during game window
  private static final int MESSAGES_COUNT = 100;

  // List usernames
  private static final String[] NAMES = {"Alice", "Bob", "Charlie", "David"};

  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

  public interface KafkaStreamingOptions extends PipelineOptions {
    /**
     * By default, this example uses Playground's Kafka server. Set this option to different value
     * to use your own Kafka server.
     */
    @Description("Kafka server host")
    @Default.String("kafka_server:9092")
    String getKafkaHost();

    void setKafkaHost(String value);
  }

  public static void main(String[] args) {
    // FixedWindows will always start at an integer multiple of the window size counting from epoch
    // start.
    // To get nicer looking results we will start producing results right after the next window
    // starts.
    Duration windowSize = Duration.standardSeconds(WINDOW_TIME);
    Instant nextWindowStart =
        new Instant(
            Instant.now().getMillis()
                + windowSize.getMillis()
                - Instant.now().plus(windowSize).getMillis() % windowSize.getMillis());

    KafkaStreamingOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaStreamingOptions.class);

    Timer timer = new Timer();

    /*
     * Kafka producer which sends messages (works in background thread)
     */
    KafkaProducer producer = new KafkaProducer(options);
    timer.schedule(producer, nextWindowStart.toDate());

    /*
     * Kafka consumer which reads messages
     */
    KafkaConsumer kafkaConsumer = new KafkaConsumer(options);
    kafkaConsumer.run();
  }

  // Kafka producer
  public static class KafkaProducer extends TimerTask {
    private final KafkaStreamingOptions options;

    public KafkaProducer(KafkaStreamingOptions options) {
      this.options = options;
    }

    @Override
    public void run() {
      Pipeline pipeline = Pipeline.create(options);

      // Generating scores with a randomly selected names and random amount of points
      PCollection<KV<String, Integer>> input =
          pipeline
              .apply(
                  GenerateSequence.from(0)
                      .withRate(MESSAGES_COUNT, Duration.standardSeconds(WINDOW_TIME))
                      .withTimestampFn((Long n) -> new Instant(System.currentTimeMillis())))
              .apply(ParDo.of(new RandomUserScoreGeneratorFn()));
      input.apply(
          KafkaIO.<String, Integer>write()
              .withBootstrapServers(options.getKafkaHost())
              .withTopic(TOPIC_NAME)
              .withKeySerializer(StringSerializer.class)
              .withValueSerializer(IntegerSerializer.class)
              .withProducerConfigUpdates(new HashMap<>()));

      pipeline.run().waitUntilFinish();
    }

    // A class that randomly selects a name with random amount of points
    static class RandomUserScoreGeneratorFn extends DoFn<Object, KV<String, Integer>> {
      private static final int MAX_SCORE = 100;

      @ProcessElement
      public void processElement(ProcessContext c) {
        c.output(generate());
      }

      public KV<String, Integer> generate() {
        Random random = new Random();
        String randomName = NAMES[random.nextInt(NAMES.length)];
        int randomScore = random.nextInt(MAX_SCORE) + 1;
        return KV.of(randomName, randomScore);
      }
    }
  }

  // Kafka consumer
  public static class KafkaConsumer {
    private final KafkaStreamingOptions options;

    public KafkaConsumer(KafkaStreamingOptions options) {
      this.options = options;
    }

    public void run() {
      Pipeline pipeline = Pipeline.create(options);

      // Create fixed window for the length of the game round
      Window<KV<String, Integer>> window =
          Window.into(FixedWindows.of(Duration.standardSeconds(WINDOW_TIME)));

      // After the first element, the trigger waits for a [TIME_OUTPUT_AFTER_FIRST_ELEMENT], after
      // which the output of elements begins
      Trigger trigger =
          AfterProcessingTime.pastFirstElementInPane()
              .plusDelayOf(Duration.standardSeconds(TIME_OUTPUT_AFTER_FIRST_ELEMENT));

      Map<String, Object> consumerConfig = new HashMap<>();

      // Start reading form Kafka with the latest offset
      consumerConfig.put("auto.offset.reset", "latest");

      PCollection<KV<String, Integer>> pCollection =
          pipeline.apply(
              KafkaIO.<String, Integer>read()
                  .withBootstrapServers(options.getKafkaHost())
                  .withTopic(TOPIC_NAME)
                  .withKeyDeserializer(StringDeserializer.class)
                  .withValueDeserializer(IntegerDeserializer.class)
                  .withConsumerConfigUpdates(consumerConfig)
                  .withoutMetadata());

      pCollection
          // Apply a window and a trigger ourput repeatedly.
          // To prevent late data from being lost, we set [withAllowedLateness].
          // To save data after each trigger is triggered [accumulatingFiredPanes] is specified.
          .apply(
              window
                  .triggering(Repeatedly.forever(trigger))
                  .withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_TIME))
                  .accumulatingFiredPanes())
          // Sum points for each player per window
          .apply(Sum.integersPerKey())
          // Combine all summed points into a single Map<>
          .apply(Combine.globally(new WindowCombineFn()).withoutDefaults())
          // Output results on the console
          .apply(ParDo.of(new LogResults()));

      pipeline.run().waitUntilFinish();
      System.out.println("Pipeline finished");
    }
  }

  static class WindowCombineFn
      extends Combine.CombineFn<KV<String, Integer>, Map<String, Integer>, Map<String, Integer>> {
    @Override
    public Map<String, Integer> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<String, Integer> addInput(
        Map<String, Integer> mutableAccumulator, KV<String, Integer> input) {
      assert input != null;
      assert mutableAccumulator != null;
      mutableAccumulator.put(input.getKey(), input.getValue());
      return mutableAccumulator;
    }

    @Override
    public Map<String, Integer> mergeAccumulators(Iterable<Map<String, Integer>> accumulators) {
      Map<String, Integer> result = new HashMap<>();
      for (Map<String, Integer> acc : accumulators) {
        for (Map.Entry<String, Integer> kv : acc.entrySet()) {
          if (result.containsKey(kv.getKey())) {
            result.put(kv.getKey(), result.get(kv.getKey()) + kv.getValue());
          } else {
            result.put(kv.getKey(), kv.getValue());
          }
        }
      }
      return result;
    }

    @Override
    public Map<String, Integer> extractOutput(Map<String, Integer> accumulator) {
      return accumulator;
    }
  }

  static class LogResults extends DoFn<Map<String, Integer>, Map<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c, IntervalWindow w) throws Exception {
      Map<String, Integer> map = c.element();
      if (map == null) {
        c.output(c.element());
        return;
      }

      String startTime = w.start().toString(dateTimeFormatter);
      String endTime = w.end().toString(dateTimeFormatter);

      PaneInfo.Timing timing = c.pane().getTiming();

      switch (timing) {
        case EARLY:
          System.out.println("Live score (running sum) for the current round:");
          break;
        case ON_TIME:
          System.out.println("Final score for the current round:");
          break;
        case LATE:
          System.out.printf("Late score for the round from %s to %s:%n", startTime, endTime);
          break;
        default:
          throw new RuntimeException("Unknown timing value");
      }

      for (Map.Entry<String, Integer> entry : map.entrySet()) {
        System.out.printf("%10s: %-10s%n", entry.getKey(), entry.getValue());
      }

      if (timing == PaneInfo.Timing.ON_TIME) {
        System.out.printf("======= End of round from %s to %s =======%n%n", startTime, endTime);
      } else {
        System.out.println();
      }

      c.output(c.element());
    }
  }
}
