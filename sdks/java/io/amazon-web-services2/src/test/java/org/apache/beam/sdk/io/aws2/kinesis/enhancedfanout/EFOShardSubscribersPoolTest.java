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
package org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout;

import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.Helpers.createConfig;
import static org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.Helpers.createReadSpec;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.kinesis.TransientKinesisException;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisClientProxyStub;
import org.apache.beam.sdk.io.aws2.kinesis.enhancedfanout.helpers.KinesisStubBehaviours;
import org.junit.Test;

public class EFOShardSubscribersPoolTest {
  @Test
  public void poolReSubscribesAndReadsRecords() throws TransientKinesisException, IOException {
    Config config = createConfig();
    KinesisIO.Read readSpec = createReadSpec();
    KinesisClientProxyStub kinesis = KinesisStubBehaviours.twoShardsWithRecords();
    KinesisReaderCheckpoint initialCheckpoint =
        new FromScratchCheckpointGenerator(config).generate(kinesis);

    EFOShardSubscribersPool pool = new EFOShardSubscribersPool(config, readSpec, kinesis);
    pool.start(initialCheckpoint);
    List<KinesisRecord> actualRecords = waitForRecords(pool, 12);
    assertEquals(12, actualRecords.size());
  }

  public static List<KinesisRecord> waitForRecords(EFOShardSubscribersPool pool, int expectedCnt)
      throws IOException {
    List<KinesisRecord> records = new ArrayList<>();
    int maxAttempts = expectedCnt * 4;
    int i = 0;
    while (i < maxAttempts) {
      KinesisRecord r = pool.getNextRecord();
      if (r != null) {
        records.add(r);
      }
      i++;
    }
    return records;
  }
}
