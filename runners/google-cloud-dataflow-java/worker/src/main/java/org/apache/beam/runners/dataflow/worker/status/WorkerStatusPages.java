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
package org.apache.beam.runners.dataflow.worker.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BooleanSupplier;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the server providing the worker status pages. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WorkerStatusPages {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerStatusPages.class);

  private final Server statusServer;
  private final List<Capturable> capturePages;
  private final StatuszServlet statuszServlet = new StatuszServlet();
  private final ThreadzServlet threadzServlet = new ThreadzServlet();
  private final ServletHandler servletHandler = new ServletHandler();

  @VisibleForTesting
  WorkerStatusPages(Server server, MemoryMonitor memoryMonitor, BooleanSupplier healthyIndicator) {
    this.statusServer = server;
    this.capturePages = new ArrayList<>();
    this.statusServer.setHandler(servletHandler);

    // Install the default servlets (threadz, healthz, heapz, jfrz, statusz)
    addServlet(threadzServlet);
    addServlet(new HealthzServlet(healthyIndicator));
    addServlet(new HeapzServlet(memoryMonitor));
    if (Environments.getJavaVersion() != Environments.JavaVersion.java8) {
      addServlet(new JfrzServlet(memoryMonitor));
    }
    addServlet(statuszServlet);

    // Add default capture pages (threadz, statusz)
    this.capturePages.add(threadzServlet);
    this.capturePages.add(statuszServlet);
    // Add some status pages
    addStatusDataProvider("resources", "Resources", memoryMonitor);
  }

  public static WorkerStatusPages create(
      int defaultStatusPort, MemoryMonitor memoryMonitor, BooleanSupplier healthyIndicator) {
    int statusPort = defaultStatusPort;
    if (System.getProperties().containsKey("status_port")) {
      statusPort = Integer.parseInt(System.getProperty("status_port"));
    }
    return new WorkerStatusPages(new Server(statusPort), memoryMonitor, healthyIndicator);
  }

  public static WorkerStatusPages create(int defaultStatusPort, MemoryMonitor memoryMonitor) {
    return create(defaultStatusPort, memoryMonitor, () -> true);
  }

  /** Start the server. */
  public void start() {
    if (statusServer.isStarted()) {
      LOG.warn("Status server already started on {}", statusServer.getURI());
      return;
    }

    try {
      addServlet(new RedirectToStatusz404Handler());
      statusServer.start();

      LOG.info("Status server started on {}", statusServer.getURI());
    } catch (Exception e) {
      LOG.warn("Status server failed to start: ", e);
    }
  }

  /** Stop the server. */
  public void stop() {
    try {
      statusServer.stop();
    } catch (Exception e) {
      LOG.warn("Status server failed to stop: ", e);
    }
  }

  /** Add a status servlet. */
  public void addServlet(BaseStatusServlet servlet) {
    ServletHolder holder = new ServletHolder();
    holder.setServlet(servlet);
    servletHandler.addServletWithMapping(holder, servlet.getPath());
  }

  /** Add data to the main statusz servlet. */
  public void addStatusDataProvider(
      String shortName, String longName, StatusDataProvider dataProvider) {
    statuszServlet.addDataProvider(shortName, longName, dataProvider);
  }

  /** Returns the set of pages than should be captured by DebugCapture. */
  public Collection<Capturable> getDebugCapturePages() {
    return this.capturePages;
  }

  public void addCapturePage(Capturable page) {
    this.capturePages.add(page);
  }

  /** Redirect all invalid pages to /statusz. */
  private static class RedirectToStatusz404Handler extends BaseStatusServlet {

    public RedirectToStatusz404Handler() {
      super("*");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
      response.sendRedirect("/statusz");
    }
  }
}
