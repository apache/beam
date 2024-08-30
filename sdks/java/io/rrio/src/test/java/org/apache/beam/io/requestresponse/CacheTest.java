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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import java.net.URI;
import org.apache.beam.io.requestresponse.CallTest.Request;
import org.apache.beam.io.requestresponse.CallTest.Response;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Cache}. */
@RunWith(JUnit4.class)
public class CacheTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenNonDeterministicCoder_readUsingRedis_throwsError()
      throws Coder.NonDeterministicException {
    URI uri = URI.create("redis://localhost:6379");
    assertThrows(
        NonDeterministicException.class,
        () ->
            Cache.readUsingRedis(
                new RedisClient(uri),
                CallTest.NON_DETERMINISTIC_REQUEST_CODER,
                CallTest.DETERMINISTIC_RESPONSE_CODER));

    assertThrows(
        NonDeterministicException.class,
        () ->
            Cache.readUsingRedis(
                new RedisClient(uri),
                CallTest.DETERMINISTIC_REQUEST_CODER,
                CallTest.NON_DETERMINISTIC_RESPONSE_CODER));

    Cache.readUsingRedis(
        new RedisClient(uri),
        CallTest.DETERMINISTIC_REQUEST_CODER,
        CallTest.DETERMINISTIC_RESPONSE_CODER);
  }

  @Test
  public void givenNonDeterministicCoder_writeUsingRedis_throwsError()
      throws Coder.NonDeterministicException {
    URI uri = URI.create("redis://localhost:6379");
    Duration expiry = Duration.standardSeconds(1L);
    assertThrows(
        NonDeterministicException.class,
        () ->
            Cache.writeUsingRedis(
                expiry,
                new RedisClient(uri),
                CallTest.NON_DETERMINISTIC_REQUEST_CODER,
                CallTest.DETERMINISTIC_RESPONSE_CODER));

    assertThrows(
        NonDeterministicException.class,
        () ->
            Cache.writeUsingRedis(
                expiry,
                new RedisClient(uri),
                CallTest.DETERMINISTIC_REQUEST_CODER,
                CallTest.NON_DETERMINISTIC_RESPONSE_CODER));

    Cache.writeUsingRedis(
        expiry,
        new RedisClient(uri),
        CallTest.DETERMINISTIC_REQUEST_CODER,
        CallTest.DETERMINISTIC_RESPONSE_CODER);
  }

  @Test
  public void givenWrongRedisURI_throwsError() throws NonDeterministicException {
    URI uri = URI.create("redis://1.2.3.4:6379");
    Duration expiry = Duration.standardSeconds(1L);
    PCollection<Request> requests =
        pipeline
            .apply("create requests", Create.of(new Request("")))
            .setCoder(CallTest.DETERMINISTIC_REQUEST_CODER);
    requests.apply(
        "readUsingRedis",
        Cache.readUsingRedis(
            new RedisClient(uri),
            CallTest.DETERMINISTIC_REQUEST_CODER,
            CallTest.DETERMINISTIC_RESPONSE_CODER));

    PCollection<KV<Request, Response>> kvs =
        pipeline.apply("create kvs", Create.of(KV.of(new Request(""), new Response(""))));
    kvs.apply(
        "writeUsingRedis",
        Cache.writeUsingRedis(
            expiry,
            new RedisClient(uri),
            CallTest.DETERMINISTIC_REQUEST_CODER,
            CallTest.DETERMINISTIC_RESPONSE_CODER));

    UncheckedExecutionException error =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(
        error.getCause().getMessage(),
        containsString("Failed to connect to host: redis://1.2.3.4:6379"));
  }
}
