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
package org.apache.beam.runners.gearpump;

import io.gearpump.cluster.ApplicationStatus;
import io.gearpump.cluster.MasterToAppMaster.AppMasterData;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.cluster.client.RunningApplication;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/** Result of executing a {@link Pipeline} with Gearpump. */
public class GearpumpPipelineResult implements PipelineResult {

  private final ClientContext client;
  private final RunningApplication app;
  private boolean finished = false;

  public GearpumpPipelineResult(ClientContext client, RunningApplication app) {
    this.client = client;
    this.app = app;
  }

  @Override
  public State getState() {
    if (!finished) {
      return getGearpumpState();
    } else {
      return State.DONE;
    }
  }

  @Override
  public State cancel() throws IOException {
    if (!finished) {
      app.shutDown();
      finished = true;
      return State.CANCELLED;
    } else {
      return State.DONE;
    }
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return waitUntilFinish();
  }

  @Override
  public State waitUntilFinish() {
    if (!finished) {
      app.waitUntilFinish();
      finished = true;
    }
    return State.DONE;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException(
        String.format("%s does not support querying metrics", getClass().getSimpleName()));
  }

  public ClientContext getClientContext() {
    return client;
  }

  private State getGearpumpState() {
    ApplicationStatus status = null;
    List<AppMasterData> apps =
        JavaConverters.seqAsJavaListConverter((Seq<AppMasterData>) client.listApps().appMasters())
            .asJava();
    for (AppMasterData appData : apps) {
      if (appData.appId() == app.appId()) {
        status = appData.status();
      }
    }
    if (null == status || status.status().equals("nonexist")) {
      return State.UNKNOWN;
    } else if (status.status().equals("active")) {
      return State.RUNNING;
    } else if (status.status().equals("succeeded")) {
      return State.DONE;
    } else {
      return State.FAILED;
    }
  }
}
