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
package org.apache.beam.sdk.io.elasticsearch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BulkIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Document;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that {@link ElasticsearchIO.BulkIO} recovers from a {@link RestClient} that stopped
 * running, without creating clients it does not need. See
 * https://github.com/apache/beam/issues/27094.
 *
 * <p>A fake Elasticsearch {@code _bulk} endpoint is served by a local HTTP server, so these tests
 * need neither Docker nor an Elasticsearch cluster.
 */
@RunWith(JUnit4.class)
public class ElasticsearchIOClientRecoveryTest {

  private static final String BULK_DIRECTIVE =
      "{\"index\":{\"_index\":\"test\",\"_id\":\"1\"}}\n{\"field\":\"value\"}\n";
  private static final String BULK_RESPONSE =
      "{\"took\":1,\"errors\":false,\"items\":"
          + "[{\"index\":{\"_index\":\"test\",\"_id\":\"1\",\"status\":201}}]}";

  private HttpServer server;
  private final AtomicInteger bulkRequests = new AtomicInteger();
  private CountingBulkIOFn fn;

  @Before
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try (InputStream body = exchange.getRequestBody()) {
            while (body.read() != -1) {
              // drain the request
            }
          }
          if (exchange.getRequestURI().getPath().endsWith("_bulk")) {
            bulkRequests.incrementAndGet();
          }
          byte[] response = BULK_RESPONSE.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(response);
          }
        });
    server.start();

    String address = "http://localhost:" + server.getAddress().getPort();
    ConnectionConfiguration connectionConfiguration =
        ConnectionConfiguration.create(new String[] {address}, "test");
    BulkIO spec = ElasticsearchIO.bulkIO().withConnectionConfiguration(connectionConfiguration);
    fn = new CountingBulkIOFn(spec);
  }

  @After
  public void tearDown() throws IOException {
    // Every client the DoFn created, including the current one; closing twice is harmless.
    for (RestClient client : fn.createdClients) {
      client.close();
    }
    server.stop(0);
  }

  @Test
  public void testStoppedClientRejectsAllRequests() throws Exception {
    // Documents the failure mode behind #27094: once the I/O reactor of a RestClient has stopped,
    // the client never serves another request.
    try (RestClient client = fn.createClient()) {
      stopIoReactor(client);

      // RestClient#performRequest wraps the IllegalStateException of the stopped async client.
      RuntimeException e =
          assertThrows(
              RuntimeException.class, () -> client.performRequest(new Request("GET", "/")));
      assertThat(e.getMessage(), containsString("I/O reactor status: STOPPED"));
    }
  }

  @Test
  public void testRunningClientIsReusedAcrossBundles() throws Exception {
    fn.setup();

    for (int i = 0; i < 100; i++) {
      fn.startBundle(null);
      fn.addAndMaybeFlush(document(), null);
      fn.finishBundle(fn.finishBundleContext());
    }

    assertEquals("no client should be created for a running client", 1, fn.clientsCreated.get());
    assertEquals(100, bulkRequests.get());
  }

  @Test
  public void testStoppedClientIsReplacedOnStartBundle() throws Exception {
    fn.setup();
    stopIoReactor(fn.currentClient());

    fn.startBundle(null);

    assertEquals(2, fn.clientsCreated.get());
    assertTrue(fn.currentClient().isRunning());

    // The replacement is healthy, so later bundles reuse it.
    for (int i = 0; i < 10; i++) {
      fn.finishBundle(fn.finishBundleContext());
      fn.startBundle(null);
    }
    assertEquals(2, fn.clientsCreated.get());
  }

  @Test
  public void testWriteSucceedsAfterClientStopsMidBundle() throws Exception {
    fn.setup();
    fn.startBundle(null);
    fn.addAndMaybeFlush(document(), null);

    // The client stops after the bundle started, so only the check before the bulk request can
    // catch it. Without that check the flush would fail with an IllegalStateException.
    stopIoReactor(fn.currentClient());
    FakeFinishBundleContext context = fn.finishBundleContext();
    fn.finishBundle(context);

    assertThat(context.outputs, hasSize(1));
    assertFalse(context.outputs.get(0).getHasError());
    assertEquals(1, bulkRequests.get());
    assertEquals(2, fn.clientsCreated.get());
    assertTrue(fn.currentClient().isRunning());
  }

  @Test
  public void testFailedReplacementIsRetriedOnNextBundle() throws Exception {
    fn.setup();
    stopIoReactor(fn.currentClient());

    fn.failNextCreate = true;
    assertThrows(IOException.class, () -> fn.startBundle(null));

    // The runner retries the bundle; the client is replaced then.
    fn.startBundle(null);
    fn.addAndMaybeFlush(document(), null);
    FakeFinishBundleContext context = fn.finishBundleContext();
    fn.finishBundle(context);

    assertThat(context.outputs, hasSize(1));
    assertFalse(context.outputs.get(0).getHasError());
    assertEquals(2, fn.clientsCreated.get());
  }

  private static Document document() {
    return Document.create().withBulkDirective(BULK_DIRECTIVE).withTimestamp(Instant.now());
  }

  /**
   * Stops the I/O reactor of {@code client} the way it happens in production: an unchecked
   * exception thrown on one of the client's I/O dispatcher threads terminates the reactor.
   */
  private static void stopIoReactor(RestClient client) throws InterruptedException {
    assertTrue(client.isRunning());
    // Listener callbacks run on an I/O dispatcher thread. RestClient and the Apache async client
    // catch Exceptions there, but not Errors (e.g. an OutOfMemoryError under load): an Error ends
    // the dispatcher thread and the I/O reactor stops for good.
    client.performRequestAsync(
        new Request("GET", "/"),
        new ResponseListener() {
          @Override
          public void onSuccess(Response response) {
            throw new Error("simulated failure on an I/O dispatcher thread");
          }

          @Override
          public void onFailure(Exception exception) {
            throw new Error("simulated failure on an I/O dispatcher thread");
          }
        });

    long deadline = System.currentTimeMillis() + 10_000;
    while (client.isRunning() && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertFalse("the I/O reactor should have stopped", client.isRunning());
  }

  /** Counts and records the clients the DoFn creates. */
  private static class CountingBulkIOFn extends BulkIO.BulkIOBundleFn {
    final AtomicInteger clientsCreated = new AtomicInteger();
    final List<RestClient> createdClients = new ArrayList<>();
    boolean failNextCreate;

    CountingBulkIOFn(BulkIO spec) {
      super(spec);
    }

    @Override
    RestClient createClient() throws IOException {
      if (failNextCreate) {
        failNextCreate = false;
        throw new IOException("simulated failure to create a client");
      }
      RestClient client = super.createClient();
      assertNotNull(client);
      clientsCreated.incrementAndGet();
      createdClients.add(client);
      return client;
    }

    /**
     * The client the DoFn currently uses. The DoFn only ever assigns its client from {@link
     * #createClient()}, so that is the most recently created one.
     */
    RestClient currentClient() {
      return createdClients.get(createdClients.size() - 1);
    }

    FakeFinishBundleContext finishBundleContext() {
      return new FakeFinishBundleContext(this);
    }
  }

  /** Collects what the DoFn outputs from {@code finishBundle}. */
  private static class FakeFinishBundleContext
      extends DoFn<Document, Document>.FinishBundleContext {
    final List<Document> outputs = new ArrayList<>();

    FakeFinishBundleContext(CountingBulkIOFn fn) {
      fn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return PipelineOptionsFactory.create();
    }

    @Override
    public void output(Document output, Instant timestamp, BoundedWindow window) {
      outputs.add(output);
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      throw new UnsupportedOperationException();
    }
  }
}
