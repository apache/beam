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
package org.apache.beam.runners.flink.examples.streaming;

import java.util.Properties;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.joda.time.Duration;

/**
 * Wordcount example using Kafka topic.
 */
public class KafkaWindowedWordCountExample {

  static final String KAFKA_TOPIC = "test";  // Default kafka topic to read from
  static final String KAFKA_BROKER = "localhost:9092";  // Default kafka broker to contact
  static final String GROUP_ID = "myGroup";  // Default groupId
  static final String ZOOKEEPER = "localhost:2181";  // Default zookeeper to connect to for Kafka

  /**
   * Function to extract words.
   */
  public static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", Sum.ofLongs());

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /**
   * Function to format KV as String.
   */
  public static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String row = c.element().getKey() + " - " + c.element().getValue() + " @ "
          + c.timestamp().toString();
      System.out.println(row);
      c.output(row);
    }
  }

  /**
   * Pipeline options.
   */
  public interface KafkaStreamingWordCountOptions
      extends WindowedWordCount.StreamingWordCountOptions {
    @Description("The Kafka topic to read from")
    @Default.String(KAFKA_TOPIC)
    String getKafkaTopic();

    void setKafkaTopic(String value);

    @Description("The Kafka Broker to read from")
    @Default.String(KAFKA_BROKER)
    String getBroker();

    void setBroker(String value);

    @Description("The Zookeeper server to connect to")
    @Default.String(ZOOKEEPER)
    String getZookeeper();

    void setZookeeper(String value);

    @Description("The groupId")
    @Default.String(GROUP_ID)
    String getGroup();

    void setGroup(String value);

  }

  public static void main(String[] args) {
    PipelineOptionsFactory.register(KafkaStreamingWordCountOptions.class);
    KafkaStreamingWordCountOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(KafkaStreamingWordCountOptions.class);
    options.setJobName("KafkaExample - WindowSize: " + options.getWindowSize() + " seconds");
    options.setStreaming(true);
    options.setCheckpointingInterval(1000L);
    options.setNumberOfExecutionRetries(5);
    options.setExecutionRetryDelay(3000L);
    options.setRunner(FlinkRunner.class);

    System.out.println(options.getKafkaTopic() + " " + options.getZookeeper() + " "
        + options.getBroker() + " " + options.getGroup());
    Pipeline pipeline = Pipeline.create(options);

    Properties p = new Properties();
    p.setProperty("zookeeper.connect", options.getZookeeper());
    p.setProperty("bootstrap.servers", options.getBroker());
    p.setProperty("group.id", options.getGroup());

    // this is the Flink consumer that reads the input to
    // the program from a kafka topic.
    FlinkKafkaConsumer08<String> kafkaConsumer = new FlinkKafkaConsumer08<>(
        options.getKafkaTopic(),
        new SimpleStringSchema(), p);

    PCollection<String> words = pipeline
        .apply("StreamingWordCount", Read.from(UnboundedFlinkSource.of(kafkaConsumer)))
        .apply(ParDo.of(new ExtractWordsFn()))
        .apply(Window.<String>into(FixedWindows.of(
            Duration.standardSeconds(options.getWindowSize())))
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());

    PCollection<KV<String, Long>> wordCounts =
        words.apply(Count.<String>perElement());

    wordCounts.apply(ParDo.of(new FormatAsStringFn()))
        .apply(TextIO.Write.to("./outputKafka.txt"));

    pipeline.run();
  }
}
