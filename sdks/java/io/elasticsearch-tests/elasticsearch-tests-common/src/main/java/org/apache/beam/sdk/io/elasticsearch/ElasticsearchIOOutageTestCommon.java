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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.createTestContainer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BulkIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Document;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.joda.time.Instant;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests that a long-lived {@link BulkIO} DoFn instance keeps writing to Elasticsearch after the
 * cluster becomes unreachable and then comes back. See https://github.com/apache/beam/issues/27094.
 *
 * <p>The DoFn reaches Elasticsearch through a Toxiproxy container, which simulates the outage: the
 * proxy refuses connections, holds them open without answering, or resets them. Each test drives
 * the same DoFn instance through a successful bundle, a bundle that fails during the outage, and a
 * bundle after the outage, as a streaming worker would. It also counts the {@link RestClient}s the
 * DoFn creates, to show that recovering does not create clients that are not needed.
 */
class ElasticsearchIOOutageTestCommon implements AutoCloseable {

  private static final DockerImageName TOXIPROXY_IMAGE =
      DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0");
  private static final String ES_NETWORK_ALIAS = "elasticsearch";
  private static final int ES_PORT = 9200;
  // The first of the proxy ports that ToxiproxyContainer exposes.
  private static final int PROXY_PORT = 8666;
  private static final String INDEX = "beam-outage-test";
  // Short, so that the hanging-connection test does not wait for the 30s default.
  private static final int SOCKET_TIMEOUT_MS = 2000;

  private final Network network;
  private final ElasticsearchContainer elasticsearch;
  private final ToxiproxyContainer toxiproxy;
  private final Proxy proxy;
  // Talks to Elasticsearch directly, not through the proxy, to set up and verify the index.
  private final RestClient directClient;
  private final ConnectionConfiguration proxiedConnectionConfiguration;

  private ElasticsearchIOOutageTestCommon(
      Network network,
      ElasticsearchContainer elasticsearch,
      ToxiproxyContainer toxiproxy,
      Proxy proxy,
      RestClient directClient,
      ConnectionConfiguration proxiedConnectionConfiguration) {
    this.network = network;
    this.elasticsearch = elasticsearch;
    this.toxiproxy = toxiproxy;
    this.proxy = proxy;
    this.directClient = directClient;
    this.proxiedConnectionConfiguration = proxiedConnectionConfiguration;
  }

  /** Starts Elasticsearch {@code imageTag} and a Toxiproxy container in front of it. */
  static ElasticsearchIOOutageTestCommon start(String imageTag) throws IOException {
    Network network = Network.newNetwork();
    ElasticsearchContainer elasticsearch = createTestContainer(imageTag);
    elasticsearch.withNetwork(network).withNetworkAliases(ES_NETWORK_ALIAS);
    elasticsearch.start();

    ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE).withNetwork(network);
    toxiproxy.start();

    Proxy proxy =
        new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort())
            .createProxy(
                "elasticsearch", "0.0.0.0:" + PROXY_PORT, ES_NETWORK_ALIAS + ":" + ES_PORT);

    RestClient directClient =
        RestClient.builder(
                new HttpHost(elasticsearch.getHost(), elasticsearch.getMappedPort(ES_PORT), "http"))
            .build();

    String proxiedAddress =
        String.format("http://%s:%d", toxiproxy.getHost(), toxiproxy.getMappedPort(PROXY_PORT));
    ConnectionConfiguration proxiedConnectionConfiguration =
        ConnectionConfiguration.create(new String[] {proxiedAddress}, INDEX)
            .withSocketTimeout(SOCKET_TIMEOUT_MS)
            .withConnectTimeout(SOCKET_TIMEOUT_MS);

    return new ElasticsearchIOOutageTestCommon(
        network, elasticsearch, toxiproxy, proxy, directClient, proxiedConnectionConfiguration);
  }

  /** Ends any outage left by a previous test and empties the index. */
  void reset() throws IOException {
    proxy.enable();
    for (Toxic toxic : proxy.toxics().getAll()) {
      toxic.remove();
    }
    ElasticsearchIOTestUtils.deleteIndex(directClient, INDEX);
  }

  @Override
  public void close() throws IOException {
    directClient.close();
    toxiproxy.stop();
    elasticsearch.stop();
    network.close();
  }

  /** Elasticsearch refuses connections, e.g. while it restarts. */
  void testRecoversAfterConnectionRefused() throws Exception {
    testRecoversAfterOutage(proxy::disable, proxy::enable);
  }

  /** Connections stay open but Elasticsearch never answers, e.g. while it is overloaded. */
  void testRecoversAfterHangingConnections() throws Exception {
    testRecoversAfterOutage(
        () -> proxy.toxics().timeout("hang", ToxicDirection.DOWNSTREAM, 0),
        () -> proxy.toxics().get("hang").remove());
  }

  /** Connections are reset, e.g. by a load balancer in front of Elasticsearch. */
  void testRecoversAfterConnectionReset() throws Exception {
    testRecoversAfterOutage(
        () -> proxy.toxics().resetPeer("reset", ToxicDirection.DOWNSTREAM, 0),
        () -> proxy.toxics().get("reset").remove());
  }

  /**
   * The I/O reactor of the DoFn's client stops. Unlike an outage, the client cannot recover from
   * this by itself, so the DoFn has to replace it.
   */
  void testRecoversAfterClientIoReactorStops() throws Exception {
    CountingBulkIOFn fn = new CountingBulkIOFn(bulkIO());
    try {
      fn.setup();
      writeBundle(fn, 1);

      stopIoReactor(fn.currentClient());
      writeBundle(fn, 2);

      assertEquals("the stopped client should be replaced once", 2, fn.clientsCreated());
      assertEquals(2, countDocuments());
    } finally {
      fn.closeAll();
    }
  }

  private void testRecoversAfterOutage(Action startOutage, Action endOutage) throws Exception {
    CountingBulkIOFn fn = new CountingBulkIOFn(bulkIO());
    try {
      fn.setup();
      writeBundle(fn, 1);

      startOutage.run();
      // The bundle fails, and the runner retries it later; the DoFn instance stays the same.
      assertThrows(IOException.class, () -> writeBundle(fn, 2));

      endOutage.run();
      writeBundle(fn, 2);

      // The client recovers from the outage by itself, so it is never replaced.
      assertEquals("no client should be created after an outage", 1, fn.clientsCreated());
      assertTrue(fn.currentClient().isRunning());
      assertEquals(2, countDocuments());
    } finally {
      fn.closeAll();
    }
  }

  private BulkIO bulkIO() {
    return ElasticsearchIO.bulkIO().withConnectionConfiguration(proxiedConnectionConfiguration);
  }

  /**
   * Runs one bundle that writes the document with {@code id}, and checks that it succeeded.
   *
   * <p>Passes null contexts: the DoFn ignores the StartBundleContext, and uses the ProcessContext
   * only to output a full batch, which a single document never fills.
   */
  @SuppressWarnings("nullness")
  private static void writeBundle(CountingBulkIOFn fn, int id) throws Exception {
    fn.startBundle(null);
    fn.addAndMaybeFlush(
        Document.create()
            .withBulkDirective(String.format("{\"index\":{\"_id\":\"%d\"}}%n{\"id\":%d}%n", id, id))
            .withTimestamp(Instant.now()),
        null);
    FakeFinishBundleContext context = new FakeFinishBundleContext(fn);
    fn.finishBundle(context);

    assertEquals(1, context.outputs.size());
    Document output = context.outputs.get(0);
    assertFalse(String.valueOf(output.getResponseItemJson()), output.getHasError());
  }

  private long countDocuments() throws IOException {
    directClient.performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    Response response = directClient.performRequest(new Request("GET", "/" + INDEX + "/_count"));
    JsonNode count = ElasticsearchIO.parseResponse(response.getEntity());
    return count.path("count").asLong();
  }

  /**
   * Stops the I/O reactor of {@code client}. Listener callbacks run on an I/O dispatcher thread.
   * RestClient and the Apache async client catch Exceptions there, but not Errors (e.g. an
   * OutOfMemoryError under load): an Error ends the dispatcher thread and the I/O reactor stops for
   * good.
   */
  private static void stopIoReactor(RestClient client) throws InterruptedException {
    assertTrue(client.isRunning());
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

  private interface Action {
    void run() throws IOException;
  }

  /** Counts and records the clients the DoFn creates. */
  private static class CountingBulkIOFn extends BulkIO.BulkIOBundleFn {
    // DoFns are Serializable, but this one is only called directly and never serialized.
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final List<RestClient> createdClients = new ArrayList<>();

    CountingBulkIOFn(BulkIO spec) {
      super(spec);
    }

    @Override
    RestClient createClient() throws IOException {
      RestClient client = super.createClient();
      createdClients.add(client);
      return client;
    }

    int clientsCreated() {
      return createdClients.size();
    }

    /**
     * The client the DoFn currently uses. The DoFn only ever assigns its client from {@link
     * #createClient()}, so that is the most recently created one.
     */
    RestClient currentClient() {
      return createdClients.get(createdClients.size() - 1);
    }

    void closeAll() throws IOException {
      for (RestClient client : createdClients) {
        client.close();
      }
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
