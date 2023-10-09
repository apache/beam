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
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** Base class for status servlets. */
public abstract class BaseStatusServlet extends HttpServlet {

  private final String path;

  protected BaseStatusServlet(String path) {
    this.path = path.startsWith("/") ? path : "/" + path;
  }

  /** Return the path that this servlet should listen on. */
  public String getPath() {
    return path;
  }

  /**
   * Return the servlet that this path is on, modified to include the specified query {@code
   * parameters} if any.
   */
  protected String getPath(String parameters) {
    if (Strings.isNullOrEmpty(parameters)) {
      return path;
    } else {
      return path + "?" + parameters;
    }
  }

  @Override
  protected abstract void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException;
}
