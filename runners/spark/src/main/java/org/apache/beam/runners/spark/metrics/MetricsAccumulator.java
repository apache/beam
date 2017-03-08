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

package org.apache.beam.runners.spark.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import java.io.IOException;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For resilience, {@link Accumulator Accumulators} are required to be wrapped in a Singleton.
 * @see <a href="https://spark.apache.org/docs/1.6.3/streaming-programming-guide.html#accumulators-and-broadcast-variables">accumulators</a>
 */
public class MetricsAccumulator {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsAccumulator.class);

  private static final String ACCUMULATOR_NAME = "Beam.Metrics";
  private static final String ACCUMULATOR_CHECKPOINT_FILENAME = "metrics";

  private static volatile Accumulator<SparkMetricsContainer> instance = null;
  private static volatile FileSystem fileSystem;
  private static volatile Path checkpointFilePath;

  /**
   * Init metrics accumulator if it has not been initiated. This method is idempotent.
   */
  public static void init(SparkPipelineOptions opts, JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (MetricsAccumulator.class) {
        if (instance == null) {
          Optional<CheckpointDir> maybeCheckpointDir =
              opts.isStreaming() ? Optional.of(new CheckpointDir(opts.getCheckpointDir()))
                  : Optional.<CheckpointDir>absent();
          Accumulator<SparkMetricsContainer> accumulator =
              jsc.sc().accumulator(new SparkMetricsContainer(), ACCUMULATOR_NAME,
                  new MetricsAccumulatorParam());
          if (maybeCheckpointDir.isPresent()) {
            Optional<SparkMetricsContainer> maybeRecoveredValue =
                recoverValueFromCheckpoint(jsc, maybeCheckpointDir.get());
            if (maybeRecoveredValue.isPresent()) {
              accumulator.setValue(maybeRecoveredValue.get());
            }
          }
          instance = accumulator;
        }
      }
      LOG.info("Instantiated metrics accumulator: " + instance.value());
    }
  }

  public static Accumulator<SparkMetricsContainer> getInstance() {
    if (instance == null) {
      throw new IllegalStateException("Metrics accumulator has not been instantiated");
    } else {
      return instance;
    }
  }

  private static Optional<SparkMetricsContainer> recoverValueFromCheckpoint(
      JavaSparkContext jsc,
      CheckpointDir checkpointDir) {
    try {
      Path beamCheckpointPath = checkpointDir.getBeamCheckpointDir();
      checkpointFilePath = new Path(beamCheckpointPath, ACCUMULATOR_CHECKPOINT_FILENAME);
      fileSystem = checkpointFilePath.getFileSystem(jsc.hadoopConfiguration());
      SparkMetricsContainer recoveredValue = Checkpoint.readObject(fileSystem, checkpointFilePath);
      if (recoveredValue != null) {
        LOG.info("Recovered metrics from checkpoint.");
        return Optional.of(recoveredValue);
      } else {
        LOG.info("No metrics checkpoint found.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure while reading metrics checkpoint.", e);
    }
    return Optional.absent();
  }

  @VisibleForTesting
  public static void clear() {
    synchronized (MetricsAccumulator.class) {
      instance = null;
    }
  }

  private static void checkpoint() throws IOException {
    if (checkpointFilePath != null) {
      Checkpoint.writeObject(fileSystem, checkpointFilePath, instance.value());
    }
  }

  /**
   * Spark Listener which checkpoints {@link SparkMetricsContainer} values for fault-tolerance.
   */
  public static class AccumulatorCheckpointingSparkListener extends JavaStreamingListener {
    @Override
    public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
      try {
        checkpoint();
      } catch (IOException e) {
        LOG.error("Failed to checkpoint metrics singleton.", e);
      }
    }
  }
}
