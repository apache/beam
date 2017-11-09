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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import redis.embedded.RedisServer;

/**
 * Test on the Redis IO.
 */
public class RedisIOTest {

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

  @Test
  public void testWriteRead() throws Exception {
    ArrayList<KV<String, String>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      KV<String, String> kv = KV.of("key " + i, "value " + i);
      data.add(kv);
    }
    PCollection<KV<String, String>> write = writePipeline.apply(Create.of(data));
    write.apply(RedisIO.write().withEndpoint("::1", embeddedRedis.getPort()));

    writePipeline.run();

    PCollection<KV<String, String>> read = readPipeline.apply("Read",
        RedisIO.read().withEndpoint("::1", embeddedRedis.getPort())
            .withKeyPattern("key*"));
    PAssert.that(read).containsInAnyOrder(data);

    PCollection<KV<String,  String>> readNotMatch = readPipeline.apply("ReadNotMatch",
        RedisIO.read().withEndpoint("::1", embeddedRedis.getPort())
            .withKeyPattern("foobar*"));
    PAssert.thatSingleton(readNotMatch.apply(Count.<KV<String, String>>globally())).isEqualTo(0L);

    readPipeline.run();
  }

  /**
   * Simple embedded Redis instance wrapper to control Redis server.
   */
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
