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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.aws2.kinesis.KinesisIO.Write.MAX_BYTES_PER_RECORD;
import static org.apache.beam.sdk.io.aws2.kinesis.RecordsAggregator.BASE_OVERHEAD;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.Duration.millis;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class RecordsAggregatorTest {
  private static final String PARTITION_KEY = "pk";
  private static final int PARTITION_KEY_OVERHEAD = 2 + PARTITION_KEY.length();
  private static final String HASH_KEY = "12345";
  private static final int HASH_KEY_OVERHEAD = 2 + HASH_KEY.length();

  private RecordsAggregator aggregator = new RecordsAggregator(MAX_BYTES_PER_RECORD, new Instant());

  @Test
  public void testAggregationCompatibilityWithKcl() {
    LongStream.range(0, 1000)
        .forEach(n -> aggregator.addRecord(PARTITION_KEY, HASH_KEY, getBytes(n)));
    byte[] aggBytes = aggregator.toBytes();
    KinesisClientRecord aggRecord = recordBuilder(aggBytes).build();

    List<KinesisClientRecord> expectedRecords =
        LongStream.range(0, 1000)
            .mapToObj(n -> recordBuilder(getBytes(n)).aggregated(true).subSequenceNumber(n).build())
            .collect(toList());

    List<KinesisClientRecord> records =
        new AggregatorUtil().deaggregate(ImmutableList.of(aggRecord));

    assertThat(records).hasSize(1000);
    assertThat(records).containsExactlyElementsOf(expectedRecords);
    assertThat(aggregator.getRecordsCount()).isEqualTo(1000);
    assertThat(aggregator.getSizeBytes()).isEqualTo(aggBytes.length);
  }

  @Test
  public void testGetRequestEntryAndReset() {
    LongStream.range(0, 1000).forEach(n -> aggregator.addRecord(PARTITION_KEY, null, getBytes(n)));
    byte[] aggBytes = aggregator.toBytes();

    Instant timeout = aggregator.timeout();
    PutRecordsRequestEntry reqEntry = aggregator.getAndReset(timeout.plus(millis(100)));
    assertThat(aggregator.timeout()).isEqualTo(timeout.plus(millis(100)));

    assertThat(reqEntry.partitionKey()).isEqualTo(PARTITION_KEY);
    assertThat(reqEntry.data().asByteArrayUnsafe()).isEqualTo(aggBytes);
    // verify reset
    assertThat(aggregator.getRecordsCount()).isEqualTo(0);
    assertThat(aggregator.toBytes()).hasSize(BASE_OVERHEAD);
    assertThat(aggregator.getSizeBytes()).isEqualTo(BASE_OVERHEAD);
  }

  @Test
  public void testSizeIncrementOfKey() {
    int increment = aggregator.sizeIncrement(PARTITION_KEY, null, null);
    System.out.println(increment);
    aggregator.addRecord(PARTITION_KEY, null, null);

    // same partition key is added only once
    System.out.println(aggregator.sizeIncrement(PARTITION_KEY, null, null));
    assertThat(increment - aggregator.sizeIncrement(PARTITION_KEY, null, null))
        .isEqualTo(PARTITION_KEY_OVERHEAD);

    increment = aggregator.sizeIncrement(PARTITION_KEY, HASH_KEY, null);
    aggregator.addRecord(PARTITION_KEY, HASH_KEY, null);

    // same hash key is added only once
    assertThat(increment - aggregator.sizeIncrement(PARTITION_KEY, HASH_KEY, null))
        .isEqualTo(HASH_KEY_OVERHEAD);
  }

  @Test
  public void testSizeIncrement() {
    Random rnd = new Random();
    List<String> keys = Stream.generate(() -> randomAscii(1, 256)).limit(3).collect(toList());
    List<String> hashKeys = Stream.generate(() -> randomNumeric(1, 38)).limit(3).collect(toList());
    hashKeys.add(null);

    int sizeBytes = BASE_OVERHEAD;
    int increment = 0;

    String pk, ehk, data;
    do {
      int currentBytes = aggregator.toBytes().length;
      assertThat(increment).isEqualTo(currentBytes - sizeBytes);
      assertThat(aggregator.getSizeBytes()).isEqualTo(currentBytes);
      sizeBytes = currentBytes;

      pk = keys.get(rnd.nextInt(keys.size()));
      ehk = hashKeys.get(rnd.nextInt(hashKeys.size()));
      data = randomAscii(256, 512);
      increment = aggregator.sizeIncrement(pk, ehk, data.getBytes(UTF_8));
    } while (aggregator.addRecord(pk, ehk, data.getBytes(UTF_8)));
  }

  @Test
  public void testRejectRecordIfSizeExceeded() {
    aggregator = new RecordsAggregator(BASE_OVERHEAD + PARTITION_KEY_OVERHEAD + 100, new Instant());
    // adding record fails due to encoding overhead
    assertThat(aggregator.addRecord(PARTITION_KEY, null, new byte[95])).isFalse();
    // but can fit if size is reduced
    assertThat(aggregator.addRecord(PARTITION_KEY, null, new byte[94])).isTrue();
  }

  @Test
  public void testHasCapacity() {
    aggregator = new RecordsAggregator(BASE_OVERHEAD + PARTITION_KEY_OVERHEAD + 100, new Instant());
    assertThat(aggregator.addRecord(PARTITION_KEY, null, new byte[30])).isTrue();
    assertThat(aggregator.hasCapacity()).isTrue(); // can fit next record of avg size
    assertThat(aggregator.addRecord(PARTITION_KEY, null, new byte[30])).isTrue();
    assertThat(aggregator.hasCapacity()).isFalse(); // can't fit next record of avg size (overhead)
    assertThat(aggregator.addRecord(PARTITION_KEY, null, new byte[30])).isFalse();
  }

  private byte[] getBytes(long n) {
    return ByteBuffer.allocate(Long.BYTES).putLong(n).array();
  }

  private KinesisClientRecord.KinesisClientRecordBuilder recordBuilder(byte[] bytes) {
    return KinesisClientRecord.builder()
        .partitionKey(PARTITION_KEY)
        .explicitHashKey(HASH_KEY)
        .data(ByteBuffer.wrap(bytes));
  }
}
