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
package org.apache.beam.sdk.io.redis;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.transform;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.embedded.RedisServer;

/** Test on the Redis IO. */
@RunWith(JUnit4.class)
public class RedisIOTest {

  private static final String REDIS_HOST = "localhost";
  private static final Long NO_EXPIRATION = -1L;

  @Rule public TestPipeline p = TestPipeline.create();

  private static RedisServer server;
  private static int port;

  private static Jedis client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    server = new RedisServer(port);
    server.start();
    client = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
  }

  @AfterClass
  public static void afterClass() {
    client.close();
    server.stop();
  }

  @Test
  public void testRead() {
    List<KV<String, String>> data = buildIncrementalData("bulkread", 10);
    data.forEach(kv -> client.set(kv.getKey(), kv.getValue()));

    PCollection<KV<String, String>> read =
        p.apply(
            "Read",
            RedisIO.read()
                .withEndpoint(REDIS_HOST, port)
                .withKeyPattern("bulkread*")
                .withBatchSize(10));
    PAssert.that(read).containsInAnyOrder(data);
    p.run();
  }

  @Test
  public void testReadSplitBig() {
    List<KV<String, String>> data = buildIncrementalData("bigset", 1000);
    data.forEach(kv -> client.set(kv.getKey(), kv.getValue()));

    PCollection<KV<String, String>> read =
        p.apply(
            "Read",
            RedisIO.read()
                .withEndpoint(REDIS_HOST, port)
                .withKeyPattern("bigset*")
                .withBatchSize(8));
    PAssert.that(read).containsInAnyOrder(data);
    p.run();
  }

  @Test
  public void testReadSplitSmall() {
    List<KV<String, String>> data = buildIncrementalData("smallset", 5);
    data.forEach(kv -> client.set(kv.getKey(), kv.getValue()));

    PCollection<KV<String, String>> read =
        p.apply(
            "Read",
            RedisIO.read()
                .withEndpoint(REDIS_HOST, port)
                .withKeyPattern("smallset*")
                .withBatchSize(20));
    PAssert.that(read).containsInAnyOrder(data);
    p.run();
  }

  @Test
  public void testReadWithKeyPattern() {
    List<KV<String, String>> data = buildIncrementalData("pattern", 10);
    data.forEach(kv -> client.set(kv.getKey(), kv.getValue()));

    PCollection<KV<String, String>> read =
        p.apply("Read", RedisIO.read().withEndpoint(REDIS_HOST, port).withKeyPattern("pattern*"));
    PAssert.that(read).containsInAnyOrder(data);

    PCollection<KV<String, String>> readNotMatch =
        p.apply(
            "ReadNotMatch",
            RedisIO.read().withEndpoint(REDIS_HOST, port).withKeyPattern("foobar*"));
    PAssert.thatSingleton(readNotMatch.apply(Count.globally())).isEqualTo(0L);

    p.run();
  }

  @Test
  public void testWriteWithMethodSet() {
    String key = "testWriteWithMethodSet";
    client.set(key, "value");

    String newValue = "newValue";
    PCollection<KV<String, String>> write = p.apply(Create.of(KV.of(key, newValue)));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.SET));
    p.run();

    assertEquals(newValue, client.get(key));
    assertEquals(NO_EXPIRATION, Long.valueOf(client.ttl(key)));
  }

  @Test
  public void testWriteWithMethodSetWithExpiration() {
    String key = "testWriteWithMethodSet";
    client.set(key, "value");

    String newValue = "newValue";
    PCollection<KV<String, String>> write = p.apply(Create.of(KV.of(key, newValue)));
    write.apply(
        RedisIO.write()
            .withEndpoint(REDIS_HOST, port)
            .withMethod(Method.SET)
            .withExpireTime(10_000L));
    p.run();

    assertEquals(newValue, client.get(key));
    Long expireTime = client.pttl(key);
    assertTrue(expireTime.toString(), 9_000 <= expireTime && expireTime <= 10_0000);
    client.del(key);
  }

  @Test
  public void testWriteWithMethodLPush() {
    String key = "testWriteWithMethodLPush";
    String value = "value";
    client.lpush(key, value);

    String newValue = "newValue";
    PCollection<KV<String, String>> write = p.apply(Create.of(KV.of(key, newValue)));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.LPUSH));
    p.run();

    List<String> values = client.lrange(key, 0, -1);
    assertEquals(newValue + value, String.join("", values));
  }

  @Test
  public void testWriteWithMethodRPush() {
    String key = "testWriteWithMethodRPush";
    String value = "value";
    client.lpush(key, value);

    String newValue = "newValue";
    PCollection<KV<String, String>> write = p.apply(Create.of(KV.of(key, newValue)));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.RPUSH));
    p.run();

    List<String> values = client.lrange(key, 0, -1);
    assertEquals(value + newValue, String.join("", values));
  }

  @Test
  public void testWriteWithMethodSAdd() {
    String key = "testWriteWithMethodSAdd";
    List<String> values = Arrays.asList("0", "1", "2", "3", "2", "4", "0", "5");
    List<KV<String, String>> data = buildConstantKeyList(key, values);

    PCollection<KV<String, String>> write = p.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.SADD));
    p.run();

    Set<String> expected = new HashSet<>(values);
    Set<String> members = client.smembers(key);
    assertEquals(expected, members);
  }

  @Test
  public void testWriteWithMethodPFAdd() {
    String key = "testWriteWithMethodPFAdd";
    List<String> values = Arrays.asList("0", "1", "2", "3", "2", "4", "0", "5");
    List<KV<String, String>> data = buildConstantKeyList(key, values);

    PCollection<KV<String, String>> write = p.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.PFADD));
    p.run();

    long count = client.pfcount(key);
    assertEquals(6, count);
    assertEquals(NO_EXPIRATION, Long.valueOf(client.ttl(key)));
  }

  @Test
  public void testWriteWithMethodPFAddWithExpireTime() {
    String key = "testWriteWithMethodPFAdd";
    List<String> values = Arrays.asList("0", "1", "2", "3", "2", "4", "0", "5");
    List<KV<String, String>> data = buildConstantKeyList(key, values);

    PCollection<KV<String, String>> write = p.apply(Create.of(data));
    write.apply(
        RedisIO.write()
            .withEndpoint(REDIS_HOST, port)
            .withMethod(Method.PFADD)
            .withExpireTime(10_000L));
    p.run();

    long count = client.pfcount(key);
    assertEquals(6, count);
    Long expireTime = client.pttl(key);
    assertTrue(expireTime.toString(), 9_000 <= expireTime && expireTime <= 10_0000);
    client.del(key);
  }

  @Test
  public void testWriteUsingINCRBY() {
    String key = "key_incr";
    List<String> values = Arrays.asList("0", "1", "2", "-3", "2", "4", "0", "5");
    List<KV<String, String>> data = buildConstantKeyList(key, values);

    PCollection<KV<String, String>> write = p.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.INCRBY));

    p.run();

    long count = Long.parseLong(client.get(key));
    assertEquals(11, count);
  }

  @Test
  public void testWriteUsingDECRBY() {
    String key = "key_decr";

    List<String> values = Arrays.asList("-10", "1", "2", "-3", "2", "4", "0", "5");
    List<KV<String, String>> data = buildConstantKeyList(key, values);

    PCollection<KV<String, String>> write = p.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, port).withMethod(Method.DECRBY));

    p.run();

    long count = Long.parseLong(client.get(key));
    assertEquals(-1, count);
  }

  @Test
  public void testWriteStreams() {

    /* test data is 10 keys (stream IDs), each with two entries, each entry having one k/v pair of data */
    List<String> redisKeys =
        IntStream.range(0, 10).boxed().map(idx -> UUID.randomUUID().toString()).collect(toList());

    Map<String, String> fooValues = ImmutableMap.of("sensor-id", "1234", "temperature", "19.8");
    Map<String, String> barValues = ImmutableMap.of("sensor-id", "9999", "temperature", "18.2");

    List<KV<String, Map<String, String>>> allData =
        redisKeys.stream()
            .flatMap(id -> Stream.of(KV.of(id, fooValues), KV.of(id, barValues)))
            .collect(toList());

    PCollection<KV<String, Map<String, String>>> write =
        p.apply(
            Create.of(allData)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(),
                        MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
    write.apply(RedisIO.writeStreams().withEndpoint(REDIS_HOST, port));
    p.run();

    for (String key : redisKeys) {
      List<StreamEntry> streamEntries =
          client.xrange(key, (StreamEntryID) null, (StreamEntryID) null, Integer.MAX_VALUE);
      assertEquals(2, streamEntries.size());
      assertThat(transform(streamEntries, StreamEntry::getFields), hasItems(fooValues, barValues));
    }
  }

  @Test
  public void testWriteStreamsWithTruncation() {
    /* test data is 10 keys (stream IDs), each with two entries, each entry having one k/v pair of data */
    List<String> redisKeys =
        IntStream.range(0, 10).boxed().map(idx -> UUID.randomUUID().toString()).collect(toList());

    Map<String, String> fooValues = ImmutableMap.of("sensor-id", "1234", "temperature", "19.8");
    Map<String, String> barValues = ImmutableMap.of("sensor-id", "9999", "temperature", "18.2");

    List<KV<String, Map<String, String>>> allData =
        redisKeys.stream()
            .flatMap(id -> Stream.of(KV.of(id, fooValues), KV.of(id, barValues)))
            .collect(toList());

    PCollection<KV<String, Map<String, String>>> write =
        p.apply(
            Create.of(allData)
                .withCoder(
                    KvCoder.of(
                        StringUtf8Coder.of(),
                        MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))));
    write.apply(
        RedisIO.writeStreams()
            .withEndpoint(REDIS_HOST, port)
            .withMaxLen(1)
            .withApproximateTrim(false));
    p.run();

    for (String stream : redisKeys) {
      long count = client.xlen(stream);
      assertEquals(1, count);
    }
  }

  @Test
  public void redisCursorToByteKey() {
    RedisCursor redisCursor = RedisCursor.of("80", 200, true);
    ByteKey byteKey = RedisCursor.redisCursorToByteKey(redisCursor);
    assertEquals(ByteKey.of(0, 0, 0, 0, 0, 0, 0, 10), byteKey);
  }

  @Test
  public void redisCursorToByteKeyZeroStart() {
    RedisCursor redisCursor = RedisCursor.of("0", 200, true);
    ByteKey byteKey = RedisCursor.redisCursorToByteKey(redisCursor);
    assertEquals(RedisCursor.ZERO_KEY, byteKey);
  }

  @Test
  public void redisCursorToByteKeyZeroEnd() {
    RedisCursor redisCursor = RedisCursor.of("0", 200, false);
    ByteKey byteKey = RedisCursor.redisCursorToByteKey(redisCursor);
    assertEquals(ByteKey.EMPTY, byteKey);
  }

  @Test
  public void redisCursorToByteKeyAndBack() {
    RedisCursor redisCursor = RedisCursor.of("80", 200, true);
    ByteKey byteKey = RedisCursor.redisCursorToByteKey(redisCursor);
    RedisCursor result = RedisCursor.byteKeyToRedisCursor(byteKey, 200, true);
    assertEquals(redisCursor.getCursor(), result.getCursor());
  }

  @Test
  public void redisByteKeyToRedisCursor() {
    ByteKey bytes = ByteKey.of(0, 0, 0, 0, 0, 25, 68, 103);
    RedisCursor redisCursor = RedisCursor.byteKeyToRedisCursor(bytes, 1048586, true);
    assertEquals("1885267", redisCursor.getCursor());
  }

  private static List<KV<String, String>> buildConstantKeyList(String key, List<String> values) {
    List<KV<String, String>> data = new ArrayList<>();
    for (String value : values) {
      data.add(KV.of(key, value));
    }
    return data;
  }

  private List<KV<String, String>> buildIncrementalData(String keyPrefix, int size) {
    List<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      data.add(KV.of(keyPrefix + i, String.valueOf(i)));
    }
    return data;
  }
}
