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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.transports.Transport;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test transport that appends accepted events to a file and can be told to reject the next few
 * emissions, standing in for a backend that is briefly unavailable.
 */
public class RejectingTransport extends Transport {

  private static final AtomicInteger REJECTIONS_REMAINING = new AtomicInteger(0);

  private final String location;

  RejectingTransport(RejectingTransportConfig config) {
    this.location = config.getLocation();
  }

  /** Makes the transport reject the next {@code count} emissions. */
  static void rejectNext(int count) {
    REJECTIONS_REMAINING.set(count);
  }

  static void reset() {
    REJECTIONS_REMAINING.set(0);
  }

  @Override
  public void emit(OpenLineage.RunEvent runEvent) {
    if (REJECTIONS_REMAINING.getAndDecrement() > 0) {
      throw new IllegalStateException("transport unavailable");
    }
    append(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(OpenLineage.DatasetEvent datasetEvent) {
    append(OpenLineageClientUtils.toJson(datasetEvent));
  }

  @Override
  public void emit(OpenLineage.JobEvent jobEvent) {
    append(OpenLineageClientUtils.toJson(jobEvent));
  }

  private void append(String json) {
    try {
      Files.write(
          Paths.get(location),
          (json + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
