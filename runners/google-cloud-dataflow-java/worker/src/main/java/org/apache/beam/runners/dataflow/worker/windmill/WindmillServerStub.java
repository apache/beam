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
package org.apache.beam.runners.dataflow.worker.windmill;

import java.io.PrintWriter;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;

/** Stub for communicating with a Windmill server. */
public abstract class WindmillServerStub
    implements ApplianceWindmillClient, StreamingEngineWindmillClient, StatusDataProvider {

  /** Returns the amount of time the server has been throttled and resets the time to 0. */
  public abstract long getAndResetThrottleTime();

  @Override
  public void appendSummaryHtml(PrintWriter writer) {}

  /** Generic Exception type for implementors to use to represent errors while making RPCs. */
  public static final class RpcException extends RuntimeException {
    public RpcException(Throwable cause) {
      super(cause);
    }
  }
}
