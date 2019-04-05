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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

/** Test on the Redis IO. */
public class RedisIOTest {

  private static final String REDIS_HOST = "::1";

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private EmbeddedRedis embeddedRedis;

  @Before
  public void before() throws Exception {
    embeddedRedis = new EmbeddedRedis();
  }

  @After
  public void after() throws Exception {
    embeddedRedis.close();
  }

  private ArrayList<KV<String, String>> ingestData(String prefix, int numKeys) {
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      KV<String, String> kv = KV.of(prefix + "-key " + i, "value " + i);
      data.add(kv);
    }
    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint("::1", embeddedRedis.getPort()));
    writePipeline.run();
    return data;
  }

  @Test
  public void testBulkRead() throws Exception {
    ArrayList<KV<String, String>> data = ingestData("bulkread", 100);
    PCollection<KV<String, String>> read =
        readPipeline.apply(
            "Read",
            RedisIO.read()
                .withEndpoint("::1", embeddedRedis.getPort())
                .withKeyPattern("bulkread*")
                .withBatchSize(10));
    PAssert.that(read).containsInAnyOrder(data);
    readPipeline.run();
  }

  @Test
  public void testWriteReadUsingDefaultAppendMethod() throws Exception {
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 8000; i++) {
      KV<String, String> kv = KV.of("key " + i, "value " + i);
      data.add(kv);
    }
    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()));

    writePipeline.run();

    PCollection<KV<String, String>> read =
        readPipeline.apply(
            "Read",
            RedisIO.read()
                .withEndpoint(REDIS_HOST, embeddedRedis.getPort())
                .withKeyPattern("key*"));
    PAssert.that(read).containsInAnyOrder(data);

    PCollection<KV<String, String>> readNotMatch =
        readPipeline.apply(
            "ReadNotMatch",
            RedisIO.read()
                .withEndpoint(REDIS_HOST, embeddedRedis.getPort())
                .withKeyPattern("foobar*"));
    PAssert.thatSingleton(readNotMatch.apply(Count.globally())).isEqualTo(0L);

    readPipeline.run();
  }

  @Test
  public void testConfiguration() {
    RedisIO.Write writeOp = RedisIO.write().withEndpoint("test", 111);
    Assert.assertEquals(111, writeOp.connectionConfiguration().port());
    Assert.assertEquals("test", writeOp.connectionConfiguration().host());
  }

  @Test
  public void testWriteReadUsingSetMethod() throws Exception {
    String key = "key";
    String value = "value";
    String newValue = "newValue";

    Jedis jedis =
        RedisConnectionConfiguration.create(REDIS_HOST, embeddedRedis.getPort()).connect();
    jedis.set(key, value);

    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(KV.of(key, newValue)));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withMethod(Method.SET));

    writePipeline.run();

    PCollection<KV<String, String>> read =
        readPipeline.apply(
            "Read",
            RedisIO.read().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withKeyPattern(key));
    PAssert.that(read).containsInAnyOrder(Collections.singletonList(KV.of(key, newValue)));

    readPipeline.run();
  }

  @Test
  public void testWriteReadUsingLpushMethod() throws Exception {
    String key = "key";
    String value = "value";
    String newValue = "newValue";

    Jedis jedis =
        RedisConnectionConfiguration.create(REDIS_HOST, embeddedRedis.getPort()).connect();
    jedis.lpush(key, value);

    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(KV.of(key, newValue)));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withMethod(Method.LPUSH));

    writePipeline.run();

    List<String> values = jedis.lrange(key, 0, -1);
    Assert.assertEquals(newValue + value, String.join("", values));
  }

  @Test
  public void testWriteReadUsingRpushMethod() throws Exception {
    String key = "key";
    String value = "value";
    String newValue = "newValue";

    Jedis jedis =
        RedisConnectionConfiguration.create(REDIS_HOST, embeddedRedis.getPort()).connect();
    jedis.lpush(key, value);

    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(KV.of(key, newValue)));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withMethod(Method.RPUSH));

    writePipeline.run();

    List<String> values = jedis.lrange(key, 0, -1);
    Assert.assertEquals(value + newValue, String.join("", values));
  }

  @Test
  public void testWriteReadUsingSaddMethod() throws Exception {
    String key = "key";

    Jedis jedis =
        RedisConnectionConfiguration.create(REDIS_HOST, embeddedRedis.getPort()).connect();

    List<String> values = Arrays.asList("0", "1", "2", "3", "2", "4", "0", "5");
    List<KV<String, String>> kvs = Lists.newArrayList();
    for (String value : values) {
      kvs.add(KV.of(key, value));
    }

    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(kvs));
    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withMethod(Method.SADD));

    writePipeline.run();

    Set<String> expected = Sets.newHashSet(values);
    Set<String> members = jedis.smembers(key);
    Assert.assertEquals(expected, members);
  }

  @Test
  public void testWriteUsingHLLMethod() throws Exception {
    String key = "key";

    Jedis jedis =
        RedisConnectionConfiguration.create(REDIS_HOST, embeddedRedis.getPort()).connect();

    PCollection<KV<String, String>> write =
        writePipeline.apply(
            Create.of(
                KV.of(key, "0"),
                KV.of(key, "1"),
                KV.of(key, "2"),
                KV.of(key, "3"),
                KV.of(key, "2"),
                KV.of(key, "4"),
                KV.of(key, "0"),
                KV.of(key, "5")));

    write.apply(
        RedisIO.write().withEndpoint(REDIS_HOST, embeddedRedis.getPort()).withMethod(Method.PFADD));

    writePipeline.run();

    long count = jedis.pfcount(key);
    Assert.assertEquals(6, count);
  }

  @Test
  public void testReadBuildsCorrectly() {
    RedisIO.Read read = RedisIO.read().withEndpoint("test", 111).withAuth("pass").withTimeout(5);
    Assert.assertEquals("test", read.connectionConfiguration().host());
    Assert.assertEquals(111, read.connectionConfiguration().port());
    Assert.assertEquals("pass", read.connectionConfiguration().auth());
    Assert.assertEquals(5, read.connectionConfiguration().timeout());
    Assert.assertEquals(false, read.connectionConfiguration().ssl());
  }

  @Test
  public void testWriteBuildsCorrectly() {
    RedisIO.Write write = RedisIO.write().withEndpoint("test", 111).withAuth("pass").withTimeout(5);
    Assert.assertEquals("test", write.connectionConfiguration().host());
    Assert.assertEquals(111, write.connectionConfiguration().port());
    Assert.assertEquals("pass", write.connectionConfiguration().auth());
    Assert.assertEquals(5, write.connectionConfiguration().timeout());
    Assert.assertEquals(false, write.connectionConfiguration().ssl());
    Assert.assertEquals(Method.APPEND, write.method());
  }

  /** Simple embedded Redis instance wrapper to control Redis server. */
  private static class EmbeddedRedis implements AutoCloseable {

    private final int port;
    private final RedisServer redisServer;

    public EmbeddedRedis() throws IOException {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        port = serverSocket.getLocalPort();
      }
      redisServer = new RedisServer(port);
      redisServer.start();
    }

    public int getPort() {
      return this.port;
    }

    @Override
    public void close() {
      redisServer.stop();
    }
  }
}
