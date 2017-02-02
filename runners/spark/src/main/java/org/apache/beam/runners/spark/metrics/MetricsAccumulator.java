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

  private static final String ACCUMULATOR_CHECKPOINT_FILENAME = "metrics";

  private static volatile Accumulator<SparkMetricsContainer> instance = null;
  private static volatile FileSystem fileSystem;
  private static volatile Path checkpointFilePath;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static void init(
      JavaSparkContext jsc,
      Optional<CheckpointDir> checkpointDir) {
    if (instance == null) {
      synchronized (MetricsAccumulator.class) {
        if (instance == null) {
          // TODO: currently when recovering from checkpoint, Spark does not recover the
          // last known Accumulator value. The SparkRunner should be able to persist and recover
          // the SparkMetricsContainer in order to recover metrics as well.
          SparkMetricsContainer initialValue = new SparkMetricsContainer();
          instance = jsc.sc().accumulator(initialValue, "Beam.Metrics",
              new MetricsAccumulatorParam());
          if (checkpointDir.isPresent()) {
            recoverValueFromCheckpoint(jsc, checkpointDir.get());
          }
        }
      }
    }
  }

  public static Accumulator<SparkMetricsContainer> getInstance() {
    if (instance == null) {
      throw new IllegalStateException("Metrics accumulator has not been instantiated");
    } else {
      return instance;
    }
  }

  private static void recoverValueFromCheckpoint(
      JavaSparkContext jsc,
      CheckpointDir checkpointDir) {
    try {
      Path beamCheckpointPath = checkpointDir.getBeamCheckpointDir();
      checkpointFilePath = new Path(beamCheckpointPath, ACCUMULATOR_CHECKPOINT_FILENAME);
      fileSystem = checkpointFilePath.getFileSystem(jsc.hadoopConfiguration());
      SparkMetricsContainer recoveredValue = Checkpoint.readObject(fileSystem, checkpointFilePath);
      if (recoveredValue != null) {
        LOG.info("Recovered metrics from checkpoint: " + recoveredValue);
        instance.setValue(recoveredValue);
      } else {
        LOG.info("No metrics checkpoint found.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure while reading metrics checkpoint.", e);
    }
  }

  @SuppressWarnings("unused")
  @VisibleForTesting
  static void clear() {
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
