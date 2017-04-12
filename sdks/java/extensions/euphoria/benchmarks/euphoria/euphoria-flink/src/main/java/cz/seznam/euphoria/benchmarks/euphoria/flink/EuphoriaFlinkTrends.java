/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.benchmarks.euphoria.flink;

import com.typesafe.config.Config;
import cz.seznam.euphoria.benchmarks.euphoria.common.trends.EuphoriaTrends;
import cz.seznam.euphoria.benchmarks.euphoria.common.trends.ExecutorFactory;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.FlinkExecutor;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class EuphoriaFlinkTrends {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + EuphoriaFlinkTrends.class + " <config-file>");
      System.exit(1);
    }
    new EuphoriaTrends(new File(args[0]), new FlinkExecutorFactory()).execute();
  }

  public static class FlinkExecutorFactory implements ExecutorFactory {

    @Override
    public Settings newFlowSettings(Config config) {
      Settings s = new Settings();
      if (config.getBoolean("flink.valueof-after-shuffle")) {
        s.setBoolean("euphoria.flink.streaming.windowing.only.after.shuffle", true);
      }
      return s;
    }

    @Override
    public Executor newExecutor(Config cfg, Collection<? extends Class<?>> dataClasses)
        throws IOException {

      // ~ get config values
      cfg = cfg.getConfig("flink");
      Duration allowedLateness = Duration.ofMillis(cfg.getDuration("allowed-lateness", TimeUnit.MILLISECONDS));
      Duration autoWatermarkInterval = Duration.ofMillis(cfg.getDuration("watermark-interval", TimeUnit.MILLISECONDS));
      Duration latencyTrackingInterval = cfg.hasPath("latency-tracking-interval")
          ? Duration.ofMillis(cfg.getDuration("latency-tracking-interval", TimeUnit.MILLISECONDS))
          : null;
      Duration checkpointInterval = cfg.hasPath("checkpoint-interval")
          ? Duration.ofMillis(cfg.getDuration("checkpoint-interval", TimeUnit.MILLISECONDS))
          : null;
      AbstractStateBackend stateBackend = cfg.hasPath("rocksdb-checkpoint")
          ? new RocksDBStateBackend(cfg.getString("rocksdb-checkpoint"))
          : null;
      boolean objectReuse = cfg.getBoolean("object-reuse");
      boolean dumpExecPlan = cfg.hasPath("dump-execution-plan") && cfg.getBoolean("dump-execution-plan");

      // ~ create the executor
      FlinkExecutor exec = new FlinkExecutor();
      exec.setDumpExecutionPlan(dumpExecPlan);
      exec.setObjectReuse(objectReuse);
      Optional.ofNullable(allowedLateness).ifPresent(exec::setAllowedLateness);
      Optional.ofNullable(autoWatermarkInterval).ifPresent(exec::setAutoWatermarkInterval);
      Optional.ofNullable(checkpointInterval).ifPresent(exec::setCheckpointInterval);
      Optional.ofNullable(stateBackend).ifPresent(exec::setStateBackend);
      Optional.ofNullable(latencyTrackingInterval).ifPresent(exec::setLatencyTrackingInterval);
      Optional.ofNullable(dataClasses).ifPresent(cls -> cls.forEach(exec::registerClass));
      return exec;
    }
  }
}
