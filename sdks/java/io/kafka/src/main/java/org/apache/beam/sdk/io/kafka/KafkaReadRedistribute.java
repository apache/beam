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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KafkaReadRedistribute<K, V>
    extends PTransform<PCollection<KafkaRecord<K, V>>, PCollection<KafkaRecord<K, V>>> {
  public static <K, V> KafkaReadRedistribute<K, V> byOffsetShard(@Nullable Integer numBuckets) {
    return new KafkaReadRedistribute<>(numBuckets, false);
  }

<<<<<<< HEAD
  public static <K, V> KafkaReadRedistribute<K, V> byRecordKey(@Nullable Integer numBuckets) {
    return new KafkaReadRedistribute<>(numBuckets, true);
=======
  public static <K, V> KafkaReadRedistribute<K, V> byRecordKey() {
    return new KafkaReadRedistribute<>(null, true);
>>>>>>> b36e2ec8b0c (Add redistribute by key variant.)
  }

  // The number of buckets to shard into.
  private @Nullable Integer numBuckets = null;
  // When redistributing, group records by the Kafka record's key instead of by offset hash.
  private boolean byRecordKey = false;

  private KafkaReadRedistribute(@Nullable Integer numBuckets, boolean byRecordKey) {
    this.numBuckets = numBuckets;
    this.byRecordKey = byRecordKey;
  }

  @Override
  public PCollection<KafkaRecord<K, V>> expand(PCollection<KafkaRecord<K, V>> input) {

    if (byRecordKey) {
      return input
<<<<<<< HEAD
          .apply("Pair with shard from key", ParDo.of(new AssignRecordKeyFn<K, V>(numBuckets)))
          .apply(Redistribute.<Integer, KafkaRecord<K, V>>byKey().withAllowDuplicates(false))
=======
          .apply("Pair with record key", ParDo.of(new AssignKeyFn<K, V>()))
          .apply(Redistribute.<K, KafkaRecord<K, V>>byKey().withAllowDuplicates(false))
>>>>>>> b36e2ec8b0c (Add redistribute by key variant.)
          .apply(Values.create());
    }

    return input
<<<<<<< HEAD
        .apply("Pair with shard from offset", ParDo.of(new AssignOffsetShardFn<K, V>(numBuckets)))
=======
        .apply("Pair with offset shard", ParDo.of(new AssignOffsetShardFn<K, V>(numBuckets)))
>>>>>>> b36e2ec8b0c (Add redistribute by key variant.)
        .apply(Redistribute.<Integer, KafkaRecord<K, V>>byKey().withAllowDuplicates(false))
        .apply(Values.create());
  }

  static class AssignOffsetShardFn<K, V>
      extends DoFn<KafkaRecord<K, V>, KV<Integer, KafkaRecord<K, V>>> {
<<<<<<< HEAD
    private @NonNull UnsignedInteger numBuckets;

    public AssignOffsetShardFn(@Nullable Integer numBuckets) {
      if (numBuckets != null && numBuckets > 0) {
        this.numBuckets = UnsignedInteger.fromIntBits(numBuckets);
      } else {
        this.numBuckets = UnsignedInteger.valueOf(0);
      }
=======
    private @Nullable Integer numBuckets;

    public AssignOffsetShardFn(@Nullable Integer numBuckets) {
      this.numBuckets = numBuckets;
>>>>>>> b36e2ec8b0c (Add redistribute by key variant.)
    }

    @ProcessElement
    public void processElement(
        @Element KafkaRecord<K, V> element,
        OutputReceiver<KV<Integer, KafkaRecord<K, V>>> receiver) {
      int hash = Hashing.farmHashFingerprint64().hashLong(element.getOffset()).asInt();

      if (numBuckets != null) {
        hash = UnsignedInteger.fromIntBits(hash).mod(numBuckets).intValue();
      }

      receiver.output(KV.of(hash, element));
    }
  }

  static class AssignRecordKeyFn<K, V>
      extends DoFn<KafkaRecord<K, V>, KV<Integer, KafkaRecord<K, V>>> {

    private @NonNull UnsignedInteger numBuckets;

    public AssignRecordKeyFn(@Nullable Integer numBuckets) {
      if (numBuckets != null && numBuckets > 0) {
        this.numBuckets = UnsignedInteger.fromIntBits(numBuckets);
      } else {
        this.numBuckets = UnsignedInteger.valueOf(0);
      }
    }

    @ProcessElement
    public void processElement(
        @Element KafkaRecord<K, V> element,
        OutputReceiver<KV<Integer, KafkaRecord<K, V>>> receiver) {
      K key = element.getKV().getKey();
      String keyString = key == null ? "" : key.toString();
      int hash = Hashing.farmHashFingerprint64().hashBytes(keyString.getBytes(UTF_8)).asInt();

      if (numBuckets != null) {
        hash = UnsignedInteger.fromIntBits(hash).mod(numBuckets).intValue();
      }

      receiver.output(KV.of(hash, element));
    }
  }

  static class AssignKeyFn<K, V> extends DoFn<KafkaRecord<K, V>, KV<K, KafkaRecord<K, V>>> {

    public AssignKeyFn() {}

    @ProcessElement
    public void processElement(
        @Element KafkaRecord<K, V> element, OutputReceiver<KV<K, KafkaRecord<K, V>>> receiver) {
      receiver.output(KV.of(element.getKV().getKey(), element));
    }
  }
}
