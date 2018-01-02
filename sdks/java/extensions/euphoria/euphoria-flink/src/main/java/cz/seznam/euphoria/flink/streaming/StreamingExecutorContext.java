/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.executor.graph.DAG;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;


public class StreamingExecutorContext
    extends ExecutorContext<StreamExecutionEnvironment, DataStream<?>> {

  private final Duration allowedLateness;

  /** True when running in local (test) mode */
  private final boolean localMode;

  public StreamingExecutorContext(StreamExecutionEnvironment env,
                                  DAG<FlinkOperator<?>> dag,
                                  FlinkAccumulatorFactory accumulatorFactory,
                                  Settings settings,
                                  Duration allowedLateness,
                                  boolean localMode) {
    super(env, dag, accumulatorFactory, settings);
    this.allowedLateness = allowedLateness;
    this.localMode = localMode;
  }

  public Duration getAllowedLateness() {
    return allowedLateness;
  }

  public boolean isLocalMode() {
    return localMode;
  }
}
