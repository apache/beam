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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.collect.Lists.newArrayList;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import java.io.Serializable;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration test, that writes and reads data to and from real Kinesis. You need to provide all
 * {@link KinesisTestOptions} in order to run this.
 */
public class KinesisIOIT implements Serializable {
  public static final int NUM_RECORDS = 1000;
  public static final int NUM_SHARDS = 2;

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public final transient TestPipeline p2 = TestPipeline.create();

  private static KinesisTestOptions options;

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(KinesisTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(KinesisTestOptions.class);
  }

  @Test
  public void testWriteThenRead() throws Exception {
    Instant now = Instant.now();
    List<byte[]> inputData = prepareData();

    // Write data into stream
    p.apply(Create.of(inputData))
        .apply(
            KinesisIO.write()
                .withStreamName(options.getAwsKinesisStream())
                .withPartitioner(new RandomPartitioner())
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion())));
    p.run().waitUntilFinish();

    // Read new data from stream that was just written before
    PCollection<byte[]> output =
        p2.apply(
                KinesisIO.read()
                    .withStreamName(options.getAwsKinesisStream())
                    .withAWSClientsProvider(
                        options.getAwsAccessKey(),
                        options.getAwsSecretKey(),
                        Regions.fromName(options.getAwsKinesisRegion()))
                    .withMaxNumRecords(inputData.size())
                    // to prevent endless running in case of error
                    .withMaxReadTime(Duration.standardMinutes(5))
                    .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                    .withInitialTimestampInStream(now)
                    .withRequestRecordsLimit(1000))
            .apply(
                ParDo.of(
                    new DoFn<KinesisRecord, byte[]>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KinesisRecord record = c.element();
                        byte[] data = record.getData().array();
                        c.output(data);
                      }
                    }));
    PAssert.that(output).containsInAnyOrder(inputData);
    p2.run().waitUntilFinish();
  }

  private List<byte[]> prepareData() {
    List<byte[]> data = newArrayList();
    for (int i = 0; i < NUM_RECORDS; i++) {
      data.add(String.valueOf(i).getBytes());
    }
    return data;
  }

  private static final class RandomPartitioner implements KinesisPartitioner {
    @Override
    public String getPartitionKey(byte[] value) {
      Random rand = new Random();
      int n = rand.nextInt(NUM_SHARDS) + 1;
      return String.valueOf(n);
    }

    @Override
    public String getExplicitHashKey(byte[] value) {
      return null;
    }
  }
}
