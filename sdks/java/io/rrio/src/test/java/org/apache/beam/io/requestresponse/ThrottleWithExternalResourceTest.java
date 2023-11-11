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
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThrottleWithExternalResource}. */
@RunWith(JUnit4.class)
public class ThrottleWithExternalResourceTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void givenNonDeterministicCoder_usingRedis_throwsError() throws NonDeterministicException {
    URI uri = URI.create("redis://localhost:6379");
    String quotaIdentifier = "quota";
    String queueKey = "queue";
    Quota quota = new Quota(10L, Duration.standardSeconds(1L));

    assertThrows(
        NonDeterministicException.class,
        () ->
            ThrottleWithExternalResource.usingRedis(
                uri, quotaIdentifier, queueKey, quota, CallTest.NON_DETERMINISTIC_REQUEST_CODER));

    ThrottleWithExternalResource.usingRedis(
        uri, quotaIdentifier, queueKey, quota, CallTest.DETERMINISTIC_REQUEST_CODER);
  }

  @Test
  public void givenWrongRedisURI_throwsError() throws NonDeterministicException {
    URI uri = URI.create("redis://1.2.3.4:6379");
    String quotaIdentifier = "quota";
    String queueKey = "queue";
    Quota quota = new Quota(10L, Duration.standardSeconds(1L));
    PCollection<Request> requests =
        pipeline.apply(Create.of(new Request(""))).setCoder(CallTest.DETERMINISTIC_REQUEST_CODER);
    requests.apply(
        ThrottleWithExternalResource.usingRedis(
            uri, quotaIdentifier, queueKey, quota, requests.getCoder()));

    UncheckedExecutionException error =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    assertThat(
        error.getCause().getMessage(),
        containsString("Failed to connect to host: redis://1.2.3.4:6379"));
  }
}
