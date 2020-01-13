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

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.fnexecution.status.BeamWorkerStatusGrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet dedicated to provide live status info retrieved from SDK Harness. Note this is different
 * from {@link WorkerStatusPages} which incorporates all info for Dataflow runner including this
 * SDKWorkerStatus page.
 */
public class SdkWorkerStatusServlet extends BaseStatusServlet implements Capturable {

  private static final Logger LOG = LoggerFactory.getLogger(SdkWorkerStatusServlet.class);
  private final transient BeamWorkerStatusGrpcService statusGrpcService;

  public SdkWorkerStatusServlet(BeamWorkerStatusGrpcService statusGrpcService) {
    super("sdk_status");
    this.statusGrpcService = statusGrpcService;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    String id = request.getParameter("id");
    if (Strings.isNullOrEmpty(id)) {
      // return all connected sdk statuses if no id provided.
      response.setContentType("text/html;charset=utf-8");
      ServletOutputStream writer = response.getOutputStream();
      try (PrintWriter out =
          new PrintWriter(new OutputStreamWriter(writer, StandardCharsets.UTF_8))) {
        captureData(out);
      }
    } else {
      response.setContentType("text/plain;charset=utf-8");
      ServletOutputStream writer = response.getOutputStream();
      writer.println(statusGrpcService.getSingleWorkerStatus(id, 10, TimeUnit.SECONDS));
    }
    response.setStatus(HttpServletResponse.SC_OK);
    response.flushBuffer();
  }

  @Override
  public String pageName() {
    return "/sdk_status";
  }

  @Override
  public void captureData(PrintWriter writer) {
    Map<String, String> allStatuses = statusGrpcService.getAllWorkerStatuses(10, TimeUnit.SECONDS);

    writer.println("<html>");
    writer.println("<h1>SDK harness</h1>");
    // add links to each sdk section for easier navigation.
    for (String sdkId : allStatuses.keySet()) {
      writer.print(String.format("<a href=\"#%s\">%s</a> ", sdkId, sdkId));
    }
    writer.println();

    for (Map.Entry<String, String> entry : allStatuses.entrySet()) {
      writer.println(String.format("<h2 id=\"%s\">%s</h2>", entry.getKey(), entry.getKey()));
      writer.println("<a href=\"#top\">return to top</a>");
      writer.println("<div style=\"white-space:pre-wrap\">");
      writer.println(entry.getValue());
      writer.println("</div>");
      writer.println("");
    }
    writer.println("</html>");
  }
}
