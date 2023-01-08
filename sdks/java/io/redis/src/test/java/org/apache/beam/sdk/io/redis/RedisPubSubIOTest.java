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

import static org.apache.beam.model.pipeline.v1.RunnerApi.StandardCoders.Enum.KV;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.embedded.RedisServer;

@RunWith(JUnit4.class)
public class RedisPubSubIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(RedisPubSubIOTest.class);
  private static final String REDIS_HOST = "localhost";

  @Rule public TestPipeline p = TestPipeline.fromOptions(testPipelineOptions());

  static TestPipelineOptions testPipelineOptions() {
    return PipelineOptionsFactory.fromArgs("--blockOnRun=false").as(TestPipelineOptions.class);
  }

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
  public void testSubscriptions() throws Exception {
    p.apply(RedisPubSubIO.subscribe().withEndpoint(REDIS_HOST, port).toChannels("test"))
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via((TimestampedValue<KV<String, String>> msg) -> msg.getValue().getValue()))
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via((String msg) -> org.apache.beam.sdk.values.KV.of("output", msg)))
        .apply(
            RedisIO.write()
                .withMethod(RedisIO.Write.Method.PUBLISH)
                .withEndpoint(REDIS_HOST, port));

    LOG.info("Starting pipeline");
    PipelineResult result = p.run();
    LOG.info("Starting listener");

    AtomicInteger other = new AtomicInteger(0);
    Thread subscriber =
        new Thread() {
          @Override
          public void run() {
            super.run();
            Jedis client = RedisConnectionConfiguration.create(REDIS_HOST, port).connect();
            client.subscribe(
                new JedisPubSub() {
                  @Override
                  public void onMessage(String channel, String message) {
                    super.onMessage(channel, message);
                    LOG.info("(" + channel + ") " + message);
                    if ("output".equals(channel)) {
                      if (other.incrementAndGet() > 4) {
                        try {
                          result.cancel();
                        } catch (Exception e) {
                          LOG.info("Pipeline cancel exception: " + e.getMessage());
                        }
                      }
                    }
                  }
                },
                "output",
                "other",
                "test");
          }
        };
    subscriber.start();
    LOG.info("Publishing messages");
    client.publish("other", "do not have");
    for (String send : ImmutableList.of("one", "two", "three", "four", "five")) {
      LOG.info("Sending message: " + send);
      client.publish("test", send);
    }
    result.waitUntilFinish();
    LOG.info("Pipeline Done");
  }
}
