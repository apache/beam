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
package org.apache.beam.sdk.io.aws2.kinesis;

import com.amazonaws.regions.Regions;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * Integration test, that writes and reads data to and from real Kinesis. You need to provide {@link
 * KinesisTestOptions} in order to run this.
 */
@RunWith(JUnit4.class)
public class KinesisIOIT implements Serializable {
  private static int numberOfShards;
  private static int numberOfRows;

  @Rule public TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public TestPipeline pipelineRead = TestPipeline.create();

  private static KinesisTestOptions options;
  private static final Instant now = Instant.now();

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(KinesisTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(KinesisTestOptions.class);
    numberOfShards = options.getNumberOfShards();
    numberOfRows = options.getNumberOfRecords();
  }

  /** Test which write and then read data for a Kinesis stream. */
  @Test
  public void testWriteThenRead() {
    runWrite();
    runRead();
  }

  /** Write test dataset into Kinesis stream. */
  private void runWrite() {
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to((long) numberOfRows))
        .apply("Prepare TestRows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
        .apply("Prepare Kinesis input records", ParDo.of(new ConvertToBytes()))
        .apply(
            "Write to Kinesis",
            org.apache.beam.sdk.io.kinesis.KinesisIO.write()
                .withStreamName(options.getAwsKinesisStream())
                .withPartitioner(new RandomPartitioner())
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion())));

    pipelineWrite.run().waitUntilFinish();
  }

  /** Read test dataset from Kinesis stream. */
  private void runRead() {
    PCollection<KinesisRecord> output =
        pipelineRead.apply(
            KinesisIO.read()
                .withStreamName(options.getAwsKinesisStream())
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Region.of(options.getAwsKinesisRegion()))
                .withMaxNumRecords(numberOfRows)
                // to prevent endless running in case of error
                .withMaxReadTime(Duration.standardMinutes(10))
                .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                .withInitialTimestampInStream(now)
                .withRequestRecordsLimit(1000));

    PAssert.thatSingleton(output.apply("Count All", Count.globally()))
        .isEqualTo((long) numberOfRows);

    PCollection<String> consolidatedHashcode =
        output
            .apply(ParDo.of(new ExtractDataValues()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numberOfRows));

    pipelineRead.run().waitUntilFinish();
  }

  /** Produces test rows. */
  private static class ConvertToBytes extends DoFn<TestRow, byte[]> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.valueOf(c.element().name()).getBytes(StandardCharsets.UTF_8));
    }
  }

  /** Read rows from Table. */
  private static class ExtractDataValues extends DoFn<KinesisRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new String(c.element().getDataAsBytes(), StandardCharsets.UTF_8));
    }
  }

  private static final class RandomPartitioner implements KinesisPartitioner {
    @Override
    public String getPartitionKey(byte[] value) {
      Random rand = new Random();
      int n = rand.nextInt(numberOfShards) + 1;
      return String.valueOf(n);
    }

    @Override
    public String getExplicitHashKey(byte[] value) {
      return null;
    }
  }
}
