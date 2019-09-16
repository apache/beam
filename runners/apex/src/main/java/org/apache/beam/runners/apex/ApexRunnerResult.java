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
package org.apache.beam.runners.apex;

import com.datatorrent.api.DAG;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/** Result of executing a {@link Pipeline} with Apex in embedded mode. */
public class ApexRunnerResult implements PipelineResult {
  private final DAG apexDAG;
  private final AppHandle apexApp;
  private State state = State.UNKNOWN;

  public ApexRunnerResult(DAG dag, AppHandle apexApp) {
    this.apexDAG = dag;
    this.apexApp = apexApp;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    apexApp.shutdown(ShutdownMode.KILL);
    cleanupOnCancelOrFinish();
    state = State.CANCELLED;
    return state;
  }

  @Override
  @Nullable
  public State waitUntilFinish(@Nullable Duration duration) {
    long timeout =
        (duration == null || duration.getMillis() < 1)
            ? Long.MAX_VALUE
            : System.currentTimeMillis() + duration.getMillis();
    try {
      while (!apexApp.isFinished() && System.currentTimeMillis() < timeout) {
        if (ApexRunner.ASSERTION_ERROR.get() != null) {
          throw ApexRunner.ASSERTION_ERROR.get();
        }
        Thread.sleep(500);
      }
      if (apexApp.isFinished()) {
        cleanupOnCancelOrFinish();
        return State.DONE;
      }
      return null;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(null);
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the DAG executed by the pipeline.
   *
   * @return DAG from translation.
   */
  public DAG getApexDAG() {
    return apexDAG;
  }

  /** Opportunity for a subclass to perform cleanup, such as removing temporary files. */
  protected void cleanupOnCancelOrFinish() {}
}
