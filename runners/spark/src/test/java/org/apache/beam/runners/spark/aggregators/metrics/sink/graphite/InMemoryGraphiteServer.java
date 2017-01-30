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

package org.apache.beam.runners.spark.aggregators.metrics.sink.graphite;

import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.rules.ExternalResource;

/**
 * A simple server that collects graphite metrics in memory.
 * This class can be used both as standalone and as a {@link org.junit.rules.TestRule},
 * in which case it handles starting, closing and clearing the collected metrics upon tests'
 * beginning and completion.
 * <P>
 * The expected packet format is string lines of the form:
 * <pre>
 *   {@code <metric path>  <metric value>  <metric timestamp>}
 * </pre>
 * See also: <a href="http://graphite.readthedocs.io/en/latest/feeding-carbon.html">Graphite's
 * plaintext protocol</a>
 * </P>
 */
public class InMemoryGraphiteServer extends ExternalResource {

  private final int port;
  private final boolean printMetrics;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private static final Map<String, Map<Long, Double>> metrics = new HashMap<>();

  public InMemoryGraphiteServer(final int port) {
    this(port, false);
  }

  public InMemoryGraphiteServer(final int port, boolean printMetrics) {
    this.port = port;
    this.printMetrics = printMetrics;
  }

  private void handleClient(final Socket clientSocket) throws IOException {
    final BufferedReader in =
        new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

    String inputLine;

    while ((inputLine = in.readLine()) != null) {
      final String[] fragments = inputLine.split(" ");
      final long timestamp = Long.parseLong(fragments[2]);
      final double value = Double.parseDouble(fragments[1]);
      final String metricName = fragments[0];

      if (printMetrics) {
        System.out.println(String.format("Timestamp: %d, name: %s, value: %s",
                                         timestamp,
                                         metricName,
                                         value));
      }

      synchronized (metrics) {
        if (!metrics.containsKey(metricName)) {
          metrics.put(metricName, new HashMap<Long, Double>());
        }

        final Map<Long, Double> metricData = metrics.get(metricName);

        metricData.put(timestamp, value);
      }
    }

    clientSocket.close();
  }

  public void close() {
    executorService.shutdownNow();
  }

  public Future<?> start() {
    return executorService.submit(new Runnable() {

      @Override
      public void run() {
        try {
          final ServerSocket socket = new ServerSocket(port);

          while (true) {
            final Socket clientSocket = socket.accept();
            executorService.submit(new Runnable() {

              @Override
              public void run() {
                try {
                  handleClient(clientSocket);
                } catch (final Exception e) {
                  e.printStackTrace();
                }
              }
            });
          }
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  public ImmutableMap<String, Map<Long, Double>> getMetrics() {
    synchronized (metrics) {
      return ImmutableMap.copyOf(metrics);
    }
  }

  @Override
  protected void before() {
    clearMetrics();
    start();
  }

  @Override
  protected void after() {
    close();
  }

  public void clearMetrics() {
    synchronized (metrics) {
      metrics.clear();
    }
  }
}
