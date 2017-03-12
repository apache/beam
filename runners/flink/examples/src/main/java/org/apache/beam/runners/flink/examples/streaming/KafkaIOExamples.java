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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSink;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Recipes/Examples that demonstrate how to read/write data from/to Kafka.
 */
public class KafkaIOExamples {


  private static final String KAFKA_TOPIC = "input";  // Default kafka topic to read from
  private static final String KAFKA_AVRO_TOPIC = "output";  // Default kafka topic to read from
  private static final String KAFKA_BROKER = "localhost:9092";  // Default kafka broker to contact
  private static final String GROUP_ID = "myGroup";  // Default groupId
  private static final String ZOOKEEPER = "localhost:2181";  // Default zookeeper to connect (Kafka)

  /**
   * Read/Write String data to Kafka.
   */
  public static class KafkaString {

    /**
     * Read String data from Kafka.
     */
    public static class ReadStringFromKafka {

      public static void main(String[] args) {

        Pipeline p = initializePipeline(args);
        KafkaOptions options = getOptions(p);

        FlinkKafkaConsumer08<String> kafkaConsumer =
            new FlinkKafkaConsumer08<>(options.getKafkaTopic(),
                new SimpleStringSchema(), getKafkaProps(options));

        p
            .apply(Read.from(UnboundedFlinkSource.of(kafkaConsumer))).setCoder(StringUtf8Coder.of())
            .apply(ParDo.of(new PrintFn<>()));

        p.run();

      }

    }

    /**
     * Write String data to Kafka.
     */
    public static class WriteStringToKafka {

      public static void main(String[] args) {

        Pipeline p = initializePipeline(args);
        KafkaOptions options = getOptions(p);

        PCollection<String> words =
            p.apply(Create.of("These", "are", "some", "words"));

        FlinkKafkaProducer08<String> kafkaSink =
            new FlinkKafkaProducer08<>(options.getKafkaTopic(),
                new SimpleStringSchema(), getKafkaProps(options));

        words.apply(Write.to(UnboundedFlinkSink.of(kafkaSink)));

        p.run();
      }

    }
  }

  /**
   * Read/Write Avro data to Kafka.
   */
  public static class KafkaAvro {

    /**
     * Read Avro data from Kafka.
     */
    public static class ReadAvroFromKafka {

      public static void main(String[] args) {

        Pipeline p = initializePipeline(args);
        KafkaOptions options = getOptions(p);

        FlinkKafkaConsumer08<MyType> kafkaConsumer =
            new FlinkKafkaConsumer08<>(options.getKafkaAvroTopic(),
                new AvroSerializationDeserializationSchema<>(MyType.class), getKafkaProps(options));

        p
            .apply(Read.from(UnboundedFlinkSource.of(kafkaConsumer)))
                .setCoder(AvroCoder.of(MyType.class))
            .apply(ParDo.of(new PrintFn<>()));

        p.run();

      }

    }

    /**
     * Write Avro data to Kafka.
     */
    public static class WriteAvroToKafka {

      public static void main(String[] args) {

        Pipeline p = initializePipeline(args);
        KafkaOptions options = getOptions(p);

        PCollection<MyType> words =
            p.apply(Create.of(
                new MyType("word", 1L),
                new MyType("another", 2L),
                new MyType("yet another", 3L)));

        FlinkKafkaProducer08<MyType> kafkaSink =
            new FlinkKafkaProducer08<>(options.getKafkaAvroTopic(),
                new AvroSerializationDeserializationSchema<>(MyType.class), getKafkaProps(options));

        words.apply(Write.to(UnboundedFlinkSink.of(kafkaSink)));

        p.run();

      }
    }

    /**
     * Serialiation/Deserialiation schema for Avro types.
     * @param <T> the type being encoded
     */
    static class AvroSerializationDeserializationSchema<T>
        implements SerializationSchema<T>, DeserializationSchema<T> {

      private final Class<T> avroType;

      private final AvroCoder<T> coder;
      private transient ByteArrayOutputStream out;

      AvroSerializationDeserializationSchema(Class<T> clazz) {
        this.avroType = clazz;
        this.coder = AvroCoder.of(clazz);
        this.out = new ByteArrayOutputStream();
      }

      @Override
      public byte[] serialize(T element) {
        if (out == null) {
          out = new ByteArrayOutputStream();
        }
        try {
          out.reset();
          coder.encode(element, out, Coder.Context.NESTED);
        } catch (IOException e) {
          throw new RuntimeException("Avro encoding failed.", e);
        }
        return out.toByteArray();
      }

      @Override
      public T deserialize(byte[] message) throws IOException {
        return coder.decode(new ByteArrayInputStream(message), Coder.Context.NESTED);
      }

      @Override
      public boolean isEndOfStream(T nextElement) {
        return false;
      }

      @Override
      public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(avroType);
      }
    }

    /**
     * Custom type for Avro serialization.
     */
    static class MyType implements Serializable {

      public MyType() {}

      MyType(String word, long count) {
        this.word = word;
        this.count = count;
      }

      String word;
      long count;

      @Override
      public String toString() {
        return "MyType{"
            + "word='" + word + '\''
            + ", count=" + count
            + '}';
      }
    }
  }

  // -------------- Utilities --------------

  /**
   * Custom options for the Pipeline.
   */
  public interface KafkaOptions extends FlinkPipelineOptions {
    @Description("The Kafka topic to read from")
    @Default.String(KAFKA_TOPIC)
    String getKafkaTopic();

    void setKafkaTopic(String value);

    void setKafkaAvroTopic(String value);

    @Description("The Kafka topic to read from")
    @Default.String(KAFKA_AVRO_TOPIC)
    String getKafkaAvroTopic();

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

  /**
   * Initializes some options for the Flink runner.
   * @param args The command line args
   * @return the pipeline
   */
  private static Pipeline initializePipeline(String[] args) {
    KafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).as(KafkaOptions.class);

    options.setStreaming(true);
    options.setRunner(FlinkRunner.class);

    options.setCheckpointingInterval(1000L);
    options.setNumberOfExecutionRetries(5);
    options.setExecutionRetryDelay(3000L);

    return Pipeline.create(options);
  }

  /**
   * Gets KafkaOptions from the Pipeline.
   * @param p the pipeline
   * @return KafkaOptions
   */
  private static KafkaOptions getOptions(Pipeline p) {
    return p.getOptions().as(KafkaOptions.class);
  }

  /**
   * Helper method to set the Kafka props from the pipeline options.
   * @param options KafkaOptions
   * @return Kafka props
   */
  private static Properties getKafkaProps(KafkaOptions options) {

    Properties props = new Properties();
    props.setProperty("zookeeper.connect", options.getZookeeper());
    props.setProperty("bootstrap.servers", options.getBroker());
    props.setProperty("group.id", options.getGroup());

    return props;
  }

  /**
   * Print contents to stdout.
   * @param <T> type of the input
   */
  private static class PrintFn<T> extends DoFn<T, T> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      System.out.println(c.element().toString());
    }
  }

}
