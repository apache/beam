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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

/** Test on the Redis IO. */
@RunWith(JUnit4.class)
public class RedisIOTest {

  private static final String REDIS_HOST = "localhost";

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
