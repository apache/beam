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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests {@link AmazonKinesisMock}.
 */
public class KinesisMockReadTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void readsDataFromMockKinesis() {
    int noOfShards = 3;
    int noOfEventsPerShard = 100;
    List<List<AmazonKinesisMock.TestData>> testData =
        provideTestData(noOfShards, noOfEventsPerShard);

    PCollection<AmazonKinesisMock.TestData> result = p
        .apply(
            KinesisIO.read()
                .withStreamName("stream")
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withAWSClientsProvider(new AmazonKinesisMock.Provider(testData, 10))
                .withMaxNumRecords(noOfShards * noOfEventsPerShard))
        .apply(ParDo.of(new KinesisRecordToTestData()));
    PAssert.that(result).containsInAnyOrder(Iterables.concat(testData));
    p.run();
  }

  static class KinesisRecordToTestData extends
      DoFn<KinesisRecord, AmazonKinesisMock.TestData> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      c.output(new AmazonKinesisMock.TestData(c.element()));
    }
  }

  private List<List<AmazonKinesisMock.TestData>> provideTestData(
      int noOfShards,
      int noOfEventsPerShard) {

    int seqNumber = 0;

    List<List<AmazonKinesisMock.TestData>> shardedData = newArrayList();
    for (int i = 0; i < noOfShards; ++i) {
      List<AmazonKinesisMock.TestData> shardData = newArrayList();
      shardedData.add(shardData);

      DateTime arrival = DateTime.now();
      for (int j = 0; j < noOfEventsPerShard; ++j) {
        arrival = arrival.plusSeconds(1);

        seqNumber++;
        shardData.add(new AmazonKinesisMock.TestData(
            Integer.toString(seqNumber),
            arrival.toInstant(),
            Integer.toString(seqNumber))
        );
      }
    }

    return shardedData;
  }
}
