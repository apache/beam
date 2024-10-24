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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.io.requestresponse.CallTest.Request;
import org.apache.beam.io.requestresponse.CallTest.Response;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class CacheIT {
  private final EchoITOptions options = readIOTestPipelineOptions(EchoITOptions.class);
  @Rule public TestPipeline writePipeline = TestPipeline.fromOptions(options);

  @Rule public TestPipeline readPipeline = TestPipeline.fromOptions(options);

  private static final String CONTAINER_IMAGE_NAME = "redis:5.0.3-alpine";
  private static final Integer PORT = 6379;

  @Rule
  public GenericContainer<?> redis =
      new GenericContainer<>(DockerImageName.parse(CONTAINER_IMAGE_NAME)).withExposedPorts(PORT);

  @Rule
  public RedisExternalResourcesRule externalClients =
      new RedisExternalResourcesRule(
          () -> {
            redis.start();
            return URI.create(
                String.format("redis://%s:%d", redis.getHost(), redis.getFirstMappedPort()));
          });

  @BeforeClass
  public static void removeIntegrationTestsProperty() {
    System.clearProperty("integrationTestPipelineOptions");
  }

  @Test
  public void givenRequestResponsesCached_writeThenReadYieldsMatches()
      throws NonDeterministicException {
    List<KV<Request, Response>> toWrite =
        ImmutableList.of(
            KV.of(new Request("a"), new Response("a")),
            KV.of(new Request("b"), new Response("b")),
            KV.of(new Request("c"), new Response("c")));
    List<Request> toRead = ImmutableList.of(new Request("a"), new Request("b"), new Request("c"));
    writeThenReadThenPAssert(toWrite, toRead, toWrite);
  }

  @Test
  public void givenNoMatchingRequestResponsePairs_yieldsKVsWithNullValues()
      throws NonDeterministicException {
    List<KV<Request, Response>> toWrite =
        ImmutableList.of(
            KV.of(new Request("a"), new Response("a")),
            KV.of(new Request("b"), new Response("b")),
            KV.of(new Request("c"), new Response("c")));
    List<Request> toRead = ImmutableList.of(new Request("d"), new Request("e"), new Request("f"));
    List<KV<Request, Response>> expected =
        toRead.stream()
            .<KV<Request, Response>>map(request -> KV.of(request, null))
            .collect(Collectors.toList());
    writeThenReadThenPAssert(toWrite, toRead, expected);
  }

  private void writeThenReadThenPAssert(
      List<KV<Request, Response>> toWrite,
      List<Request> toRead,
      List<KV<Request, Response>> expected)
      throws NonDeterministicException {
    PCollection<KV<Request, Response>> toWritePCol = writePipeline.apply(Create.of(toWrite));
    toWritePCol.apply(
        Cache.writeUsingRedis(
            Duration.standardHours(1L),
            externalClients.getActualClient(),
            CallTest.DETERMINISTIC_REQUEST_CODER,
            CallTest.DETERMINISTIC_RESPONSE_CODER));

    PCollection<Request> requests =
        readPipeline.apply(Create.of(toRead)).setCoder(CallTest.DETERMINISTIC_REQUEST_CODER);

    Result<KV<Request, Response>> gotKVsResult =
        requests.apply(
            Cache.readUsingRedis(
                externalClients.getActualClient(),
                CallTest.DETERMINISTIC_REQUEST_CODER,
                CallTest.DETERMINISTIC_RESPONSE_CODER));

    PAssert.that(gotKVsResult.getFailures()).empty();
    PAssert.that(gotKVsResult.getResponses()).containsInAnyOrder(expected);

    writePipeline.run().waitUntilFinish();
    readPipeline.run();
  }
}
