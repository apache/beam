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

package org.apache.beam.sdk.fn.data;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CompletableFutureInboundDataClient}.
 */
@RunWith(JUnit4.class)
public class CompletableFutureInboundDataClientTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testComplete() throws Exception {
    InboundDataClient client = CompletableFutureInboundDataClient.create();

    assertThat(client.isDone(), is(false));

    client.complete();

    assertThat(client.isDone(), is(true));
    // Should return immediately
    client.awaitCompletion();
  }

  @Test
  public void testCanceled() throws Exception {
    InboundDataClient client = CompletableFutureInboundDataClient.create();

    assertThat(client.isDone(), is(false));

    client.cancel();

    assertThat(client.isDone(), is(true));

    thrown.expect(CancellationException.class);
    // Should return immediately
    client.awaitCompletion();
  }

  @Test
  public void testFailed() throws Exception {
    InboundDataClient client = CompletableFutureInboundDataClient.create();

    assertThat(client.isDone(), is(false));

    client.fail(new UnsupportedOperationException("message"));

    assertThat(client.isDone(), is(true));

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(UnsupportedOperationException.class));
    thrown.expectMessage("message");
    client.awaitCompletion();
  }

  @Test
  public void testCompleteMultithreaded() throws Exception {
    InboundDataClient client = CompletableFutureInboundDataClient.create();
    Future<Void> waitingFuture =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  client.awaitCompletion();
                  return null;
                });

    try {
      waitingFuture.get(50, TimeUnit.MILLISECONDS);
    } catch (TimeoutException expected) {
      // This should time out, as the client should never complete without external completion
    }

    client.complete();
    // Blocks forever if the thread does not continue
    waitingFuture.get();
  }

  @Test
  public void testCompleteBackingFuture() throws Exception {
    CompletableFuture<Object> future = new CompletableFuture<>();
    InboundDataClient client = CompletableFutureInboundDataClient.forBackingFuture(future);

    assertThat(future.isDone(), is(false));
    assertThat(client.isDone(), is(false));

    client.complete();

    assertThat(future.isDone(), is(true));
    assertThat(client.isDone(), is(true));
    // Should return immediately
    client.awaitCompletion();
  }

  @Test
  public void testCancelBackingFuture() throws Exception {
    CompletableFuture<Object> future = new CompletableFuture<>();
    InboundDataClient client = CompletableFutureInboundDataClient.forBackingFuture(future);

    assertThat(future.isDone(), is(false));
    assertThat(client.isDone(), is(false));

    client.cancel();

    assertThat(future.isDone(), is(true));
    assertThat(client.isDone(), is(true));
    assertThat(future.isCancelled(), is(true));
    thrown.expect(CancellationException.class);
    // Should return immediately
    future.get();
  }

  @Test
  public void testFailBackingFuture() throws Exception {
    CompletableFuture<Object> future = new CompletableFuture<>();
    InboundDataClient client = CompletableFutureInboundDataClient.forBackingFuture(future);

    assertThat(future.isDone(), is(false));
    assertThat(client.isDone(), is(false));

    client.fail(new UnsupportedOperationException("message"));

    assertThat(client.isDone(), is(true));
    assertThat(future.isDone(), is(true));
    assertThat(future.isCompletedExceptionally(), is(true));

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(UnsupportedOperationException.class));
    thrown.expectMessage("message");
    client.awaitCompletion();
  }
}
