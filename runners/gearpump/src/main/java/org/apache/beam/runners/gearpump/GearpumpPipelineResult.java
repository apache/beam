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

import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;

import org.apache.gearpump.cluster.MasterToAppMaster;
import org.apache.gearpump.cluster.MasterToAppMaster.AppMasterData;
import org.apache.gearpump.cluster.client.ClientContext;
import org.joda.time.Duration;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Result of executing a {@link Pipeline} with Gearpump.
 */
public class GearpumpPipelineResult implements PipelineResult {

  private final ClientContext client;
  private final int appId;
  private final Duration defaultWaitDuration = Duration.standardSeconds(60);
  private final Duration defaultWaitInterval = Duration.standardSeconds(10);

  public GearpumpPipelineResult(ClientContext client, int appId) {
    this.client = client;
    this.appId = appId;
  }

  @Override
  public State getState() {
    return getGearpumpState();
  }

  @Override
  public State cancel() throws IOException {
    client.shutdown(appId);
    return State.CANCELLED;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    long start = System.currentTimeMillis();
    do {
      try {
        Thread.sleep(defaultWaitInterval.getMillis());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } while (State.RUNNING == getGearpumpState()
        && (System.currentTimeMillis() - start) < duration.getMillis());

    if (State.RUNNING == getGearpumpState()) {
      return State.DONE;
    } else {
      return State.FAILED;
    }
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(defaultWaitDuration);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    throw new AggregatorRetrievalException(
        "PipelineResult getAggregatorValues not supported in Gearpump pipeline",
        new UnsupportedOperationException());
  }

  @Override
  public MetricResults metrics() {
    return null;
  }

  private State getGearpumpState() {
    String status = null;
    List<AppMasterData> apps =
        JavaConverters.<AppMasterData>seqAsJavaListConverter(
            (Seq<AppMasterData>) client.listApps().appMasters()).asJava();
    for (AppMasterData app: apps) {
      if (app.appId() == appId) {
        status = app.status();
      }
    }
    if (null == status || status.equals(MasterToAppMaster.AppMasterNonExist())) {
      return State.UNKNOWN;
    } else if (status.equals(MasterToAppMaster.AppMasterActive())) {
      return State.RUNNING;
    } else {
      return State.STOPPED;
    }
  }

}
