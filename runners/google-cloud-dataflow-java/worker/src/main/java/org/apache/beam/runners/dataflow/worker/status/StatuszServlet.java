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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;

/**
 * General servlet for providing a bunch of information on the statusz page.
 *
 * <p><b>Not actually serializable</b>. Its superclass is serializable but this subclass is not.
 */
@SuppressFBWarnings("SE_BAD_FIELD") // not serializable
public class StatuszServlet extends BaseStatusServlet implements Capturable {

  private static class DataProviderInfo {
    private final String longName;
    private final StatusDataProvider dataProvider;

    public DataProviderInfo(String longName, StatusDataProvider dataProvider) {
      this.longName = longName;
      this.dataProvider = dataProvider;
    }
  }

  private LinkedHashMap<String, DataProviderInfo> dataProviders = new LinkedHashMap<>();

  public StatuszServlet() {
    super("statusz");
  }

  public void addDataProvider(String shortName, String longName, StatusDataProvider provider) {
    dataProviders.put(shortName, new DataProviderInfo(longName, provider));
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    response.setStatus(HttpServletResponse.SC_OK);
    PrintWriter writer = response.getWriter();
    captureData(writer);
  }

  @Override
  public String pageName() {
    return "/statusz";
  }

  @Override
  public void captureData(PrintWriter writer) {
    writer.println("<html>");

    writer.println("<h1>Worker Harness</h1>");

    for (DataProviderInfo info : dataProviders.values()) {
      writer.print("<h2>");
      writer.print(info.longName);
      writer.println("</h2>");

      info.dataProvider.appendSummaryHtml(writer);
    }
    writer.println("</html>");
  }
}
