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

import static org.junit.Assert.assertThrows;

import java.net.URI;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Cache}. */
@RunWith(JUnit4.class)
public class CacheTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenNonDeterministicCoder_UsingRedis_throwsError()
      throws Coder.NonDeterministicException {
    assertThrows(
        "Request Coder is non-deterministic",
        Coder.NonDeterministicException.class,
        () ->
            Cache.usingRedis(
                SerializableCoder.of(CallTest.Request.class),
                StringUtf8Coder.of(),
                new RedisClient(URI.create("redis://localhost:6379"))));

    assertThrows(
        "Response Coder is non-deterministic",
        Coder.NonDeterministicException.class,
        () ->
            Cache.usingRedis(
                StringUtf8Coder.of(),
                CallTest.RESPONSE_CODER,
                new RedisClient(URI.create("redis://localhost:6379"))));

    // both Request and Response Coders are deterministic
    Cache.usingRedis(
        StringUtf8Coder.of(),
        StringUtf8Coder.of(),
        new RedisClient(URI.create("redis://localhost:6379")));
  }

  @Test
  public void givenUsingRedis_pipelineConstructsDoesNotThrowError() {
  }

  @Test
  public void givenWrongRedisURI_throwsError() {}


  private static class ValidCaller
      implements Caller<
              CallTest.@NonNull Request,
              @NonNull KV<CallTest.@NonNull Request, CallTest.@Nullable Response>>,
          SetupTeardown {
    private final @NonNull KV<CallTest.@NonNull Request, CallTest.@Nullable Response> yieldValue;

    private ValidCaller(
        @NonNull KV<CallTest.@NonNull Request, CallTest.@Nullable Response> yieldValue) {
      this.yieldValue = yieldValue;
    }

    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}

    @Override
    public @NonNull KV<CallTest.@NonNull Request, CallTest.@Nullable Response> call(
        CallTest.@NonNull Request request) throws UserCodeExecutionException {
      return yieldValue;
    }
  }
}
