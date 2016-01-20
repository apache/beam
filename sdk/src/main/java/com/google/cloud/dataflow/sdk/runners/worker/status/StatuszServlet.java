/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker.status;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * General servlet for providing a bunch of information on the statusz page.
 */
public class StatuszServlet extends BaseStatusServlet {

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
    PrintWriter writer = response.getWriter();
    writer.println("<html>");

    writer.println("<h1>Worker Harness</h1>");

    for (DataProviderInfo info : dataProviders.values()) {
      writer.print("<h2>");
      writer.print(info.longName);
      writer.println("</h2>");

      info.dataProvider.appendSummaryHtml(writer);
    }
    writer.println("<html>");
  }
}
