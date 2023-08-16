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
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

/**
 * Respond to /jfrz with a page allowing downloading of the JFR profiles.
 *
 * <p><b>Not actually serializable</b>. Its superclass is serializable but this subclass is not.
 */
@SuppressFBWarnings("SE_BAD_FIELD") // not serializable
public class JfrzServlet extends BaseStatusServlet {
  private static final Duration DEFAULT_RECORDING_DURATION = Duration.ofMinutes(1);

  private final MemoryMonitor memoryMonitor;

  public JfrzServlet(MemoryMonitor memoryMonitor) {
    super("jfrz");
    this.memoryMonitor = memoryMonitor;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String durationStr = req.getParameter("duration");
    Duration duration;
    if (durationStr == null) {
      duration = DEFAULT_RECORDING_DURATION;
    } else {
      duration = Duration.parse(durationStr);
    }

    ServletOutputStream writer = resp.getOutputStream();

    try (InputStream jfrStream = memoryMonitor.runJfrProfile(duration).get()) {
      resp.setContentType("application/octet-stream");
      resp.setHeader("Content-Disposition", "attachment; filename=\"profile.jfr\"");
      ByteStreams.copy(jfrStream, writer);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    resp.setStatus(HttpServletResponse.SC_OK);
  }
}
