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

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicReference;

/** Capture the last exception thrown during processing, and display that on the statusz page. */
public class LastExceptionDataProvider implements StatusDataProvider {

  private static final AtomicReference<Throwable> lastException = new AtomicReference<>();

  /** Report an exception to the exception data provider. */
  public static void reportException(Throwable t) {
    lastException.set(t);
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    Throwable t = lastException.get();
    if (t == null) {
      writer.println("None<br>");
    } else {
      writer.println("<pre>");
      t.printStackTrace(writer);
      writer.println("</pre>");
    }
  }
}
