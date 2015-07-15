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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper around a {@link WindmillServerStub} that tracks metrics for the number of in-flight
 * requests.
 */
public class MetricTrackingWindmillServerStub {

  private final AtomicInteger activeSideInputs = new AtomicInteger();
  private final AtomicInteger activeStateReads = new AtomicInteger();
  private final WindmillServerStub server;

  public MetricTrackingWindmillServerStub(WindmillServerStub server) {
    this.server = server;
  }

  public Windmill.GetDataResponse getStateData(Windmill.GetDataRequest request) {
    activeStateReads.getAndIncrement();
    try {
      return server.getData(request);
    } finally {
      activeStateReads.getAndDecrement();
    }
  }

  public Windmill.GetDataResponse getSideInputData(Windmill.GetDataRequest request) {
    activeSideInputs.getAndIncrement();
    try {
      return server.getData(request);
    } finally {
      activeSideInputs.getAndDecrement();
    }
  }

  public void printHtml(PrintWriter writer) {
    writer.println("Active Fetches:");
    writer.println("  Side Inputs: " + activeSideInputs.get());
    writer.println("  State Reads: " + activeStateReads.get());
  }

  public AutoCloseable sideInput() {
    return initiate(activeSideInputs);
  }

  private AutoCloseable initiate(final AtomicInteger counter) {
    counter.getAndIncrement();
    return new AutoCloseable() {
      @Override
      public void close() {
        counter.getAndDecrement();
      }
    };
  }
}
