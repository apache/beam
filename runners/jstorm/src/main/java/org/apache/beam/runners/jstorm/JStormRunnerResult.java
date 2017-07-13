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
package org.apache.beam.runners.jstorm;

import static com.google.common.base.Preconditions.checkNotNull;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import com.alibaba.jstorm.utils.JStormUtils;
import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * A {@link PipelineResult} of executing {@link org.apache.beam.sdk.Pipeline Pipelines} using
 * {@link JStormRunner}.
 */
public abstract class JStormRunnerResult implements PipelineResult {

  public static JStormRunnerResult local(
      String topologyName,
      Config config,
      LocalCluster localCluster,
      long localModeExecuteTimeSecs) {
    return new LocalStormPipelineResult(
        topologyName, config, localCluster, localModeExecuteTimeSecs);
  }

  private final String topologyName;
  private final Config config;

  JStormRunnerResult(String topologyName, Config config) {
    this.config = checkNotNull(config, "config");
    this.topologyName = checkNotNull(topologyName, "topologyName");
  }

  public State getState() {
    return null;
  }

  public Config getConfig() {
    return config;
  }

  public String getTopologyName() {
    return topologyName;
  }

  private static class LocalStormPipelineResult extends JStormRunnerResult {

    private LocalCluster localCluster;
    private long localModeExecuteTimeSecs;

    LocalStormPipelineResult(
        String topologyName,
        Config config,
        LocalCluster localCluster,
        long localModeExecuteTimeSecs) {
      super(topologyName, config);
      this.localCluster = checkNotNull(localCluster, "localCluster");
    }

    @Override
    public State cancel() throws IOException {
      //localCluster.deactivate(getTopologyName());
      localCluster.killTopology(getTopologyName());
      localCluster.shutdown();
      JStormUtils.sleepMs(1000);
      return State.CANCELLED;
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      return waitUntilFinish();
    }

    @Override
    public State waitUntilFinish() {
      JStormUtils.sleepMs(localModeExecuteTimeSecs * 1000);
      try {
        return cancel();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public MetricResults metrics() {
      return null;
    }
  }
}
