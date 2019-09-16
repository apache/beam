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
import java.util.function.BooleanSupplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Respond to /healthz with health information. */
public class HealthzServlet extends BaseStatusServlet {

  private final BooleanSupplier healthyIndicator;

  public HealthzServlet(BooleanSupplier healthyIndicator) {
    super("healthz");
    this.healthyIndicator = healthyIndicator;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("text/html;charset=utf-8");
    if (healthyIndicator.getAsBoolean()) {
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().println("ok");
    } else {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      response.getWriter().println("internal server error");
    }
  }
}
