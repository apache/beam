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
package org.apache.beam.runners.spark.aggregators;

import java.io.IOException;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For resilience, {@link AccumulatorV2 Accumulators} are required to be wrapped in a Singleton.
 *
 * @see <a
 *     href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/util/AccumulatorV2.html">accumulatorsV2</a>
 */
public class AggregatorsAccumulator {
  private static final Logger LOG = LoggerFactory.getLogger(AggregatorsAccumulator.class);

  private static final String ACCUMULATOR_NAME = "Beam.Aggregators";
  private static final String ACCUMULATOR_CHECKPOINT_FILENAME = "aggregators";

  private static volatile NamedAggregatorsAccumulator instance = null;
  private static volatile FileSystem fileSystem;
  private static volatile Path checkpointFilePath;

  /** Init aggregators accumulator if it has not been initiated. This method is idempotent. */
  public static void init(SparkPipelineOptions opts, JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (AggregatorsAccumulator.class) {
        if (instance == null) {
          Optional<CheckpointDir> maybeCheckpointDir =
              opts.isStreaming()
                  ? Optional.of(new CheckpointDir(opts.getCheckpointDir()))
                  : Optional.absent();
          NamedAggregators namedAggregators = new NamedAggregators();
          NamedAggregatorsAccumulator accumulator =
              new NamedAggregatorsAccumulator(namedAggregators);

          if (maybeCheckpointDir.isPresent()) {
            Optional<NamedAggregators> maybeRecoveredValue =
                recoverValueFromCheckpoint(jsc, maybeCheckpointDir.get());
            if (maybeRecoveredValue.isPresent()) {
              accumulator = new NamedAggregatorsAccumulator(maybeRecoveredValue.get());
            }
          }
          jsc.sc().register(accumulator, ACCUMULATOR_NAME);
          instance = accumulator;
        }
      }
      LOG.info("Instantiated aggregators accumulator: " + instance.value());
    }
  }

  public static NamedAggregatorsAccumulator getInstance() {
    if (instance == null) {
      throw new IllegalStateException("Aggregrators accumulator has not been instantiated");
    } else {
      return instance;
    }
  }

  private static Optional<NamedAggregators> recoverValueFromCheckpoint(
      JavaSparkContext jsc, CheckpointDir checkpointDir) {
    try {
      Path beamCheckpointPath = checkpointDir.getBeamCheckpointDir();
      checkpointFilePath = new Path(beamCheckpointPath, ACCUMULATOR_CHECKPOINT_FILENAME);
      fileSystem = checkpointFilePath.getFileSystem(jsc.hadoopConfiguration());
      NamedAggregators recoveredValue = Checkpoint.readObject(fileSystem, checkpointFilePath);
      if (recoveredValue != null) {
        LOG.info("Recovered aggregators from checkpoint");
        return Optional.of(recoveredValue);
      } else {
        LOG.info("No accumulator checkpoint found.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure while reading accumulator checkpoint.", e);
    }
    return Optional.absent();
  }

  private static void checkpoint() throws IOException {
    if (checkpointFilePath != null) {
      Checkpoint.writeObject(fileSystem, checkpointFilePath, instance.value());
    }
  }

  @VisibleForTesting
  public static void clear() {
    synchronized (AggregatorsAccumulator.class) {
      instance = null;
    }
  }

  /** Spark Listener which checkpoints {@link NamedAggregators} values for fault-tolerance. */
  public static class AccumulatorCheckpointingSparkListener extends JavaStreamingListener {
    @Override
    public void onBatchCompleted(JavaStreamingListenerBatchCompleted batchCompleted) {
      try {
        checkpoint();
      } catch (IOException e) {
        LOG.error("Failed to checkpoint accumulator singleton.", e);
      }
    }
  }
}
