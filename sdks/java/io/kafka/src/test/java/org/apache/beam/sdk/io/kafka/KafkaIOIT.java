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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromJsonString;

import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * IO Integration test for {@link org.apache.beam.sdk.io.kafka.KafkaIO}.
 *
 * <p>{@see https://beam.apache.org/documentation/io/testing/#i-o-transform-integration-tests} for
 * more details.
 *
 * <p>NOTE: This test sets retention policy of the messages so that all messages are retained in the
 * topic so that we could read them back after writing.
 */
@RunWith(JUnit4.class)
public class KafkaIOIT {

  /** Hash for 1000 uniformly distributed records with 10B keys and 90B values (100kB total). */
  private static final String EXPECTED_HASHCODE = "4507649971ee7c51abbb446e65a5c660";

  private static SyntheticSourceOptions sourceOptions;

  private static Options options;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(Options.class);
    sourceOptions = fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
  }

  @Test
  public void testKafkaIOReadsAndWritesCorrectly() throws IOException {
    writePipeline
        .apply("Generate records", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Write to Kafka", writeToKafka());

    writePipeline.run().waitUntilFinish();

    PCollection<String> hashcode =
        readPipeline
            .apply("Read from Kafka", readFromKafka())
            .apply("Map records to strings", MapElements.via(new MapKafkaRecordsToStrings()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.thatSingleton(hashcode).isEqualTo(EXPECTED_HASHCODE);

    PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));
    cancelIfNotTerminal(readResult, readState);
  }

  private void cancelIfNotTerminal(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {
    if (!readState.isTerminal()) {
      readResult.cancel();
    }
  }

  private KafkaIO.Write<byte[], byte[]> writeToKafka() {
    return KafkaIO.<byte[], byte[]>write()
        .withBootstrapServers(options.getKafkaBootstrapServerAddress())
        .withTopic(options.getKafkaTopic())
        .withKeySerializer(ByteArraySerializer.class)
        .withValueSerializer(ByteArraySerializer.class);
  }

  private KafkaIO.Read<byte[], byte[]> readFromKafka() {
    return KafkaIO.readBytes()
        .withBootstrapServers(options.getKafkaBootstrapServerAddress())
        .withConsumerConfigUpdates(ImmutableMap.of("auto.offset.reset", "earliest"))
        .withTopic(options.getKafkaTopic())
        .withMaxNumRecords(sourceOptions.numRecords);
  }

  /** Pipeline options specific for this test. */
  public interface Options extends PipelineOptions, StreamingOptions {

    @Description("Options for synthetic source.")
    @Validation.Required
    String getSourceOptions();

    void setSourceOptions(String sourceOptions);

    @Description("Kafka server address")
    @Validation.Required
    String getKafkaBootstrapServerAddress();

    void setKafkaBootstrapServerAddress(String address);

    @Description("Kafka topic")
    @Validation.Required
    String getKafkaTopic();

    void setKafkaTopic(String topic);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);
  }

  private static class MapKafkaRecordsToStrings
      extends SimpleFunction<KafkaRecord<byte[], byte[]>, String> {
    @Override
    public String apply(KafkaRecord<byte[], byte[]> input) {
      String key = Arrays.toString(input.getKV().getKey());
      String value = Arrays.toString(input.getKV().getValue());
      return String.format("%s %s", key, value);
    }
  }
}
