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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.beam.runners.spark.translation.streaming.CheckpointDir;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For resilience, {@link Accumulator}s are required to be wrapped in a Singleton.
 * @see <a href="https://spark.apache.org/docs/1.6.3/streaming-programming-guide.html#accumulators-and-broadcast-variables">accumulators</a>
 */
public class AccumulatorSingleton {
  private static final Logger LOG = LoggerFactory.getLogger(AccumulatorSingleton.class);

  private static final String ACCUMULATOR_CHECKPOINT_FILENAME = "beam_aggregators";

  private static volatile Accumulator<NamedAggregators> instance;
  private static volatile FileSystem fileSystem;
  private static volatile Path checkpointPath;
  private static volatile Path tempCheckpointPath;
  private static volatile Path backupCheckpointPath;

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  static Accumulator<NamedAggregators> getInstance(
      JavaSparkContext jsc,
      Optional<CheckpointDir> checkpointDir) {
    if (instance == null) {
      synchronized (AccumulatorSingleton.class) {
        if (instance == null) {
          instance = jsc.sc().accumulator(new NamedAggregators(), new AggAccumParam());
          if (checkpointDir.isPresent()) {
            recoverValueFromCheckpoint(jsc, checkpointDir.get());
          }
        }
      }
    }
    return instance;
  }

  private static void recoverValueFromCheckpoint(
      JavaSparkContext jsc,
      CheckpointDir checkpointDir) {
    FSDataInputStream is = null;
    try {
      Path beamCheckpointPath = checkpointDir.getBeamCheckpointDir();
      checkpointPath = new Path(beamCheckpointPath, ACCUMULATOR_CHECKPOINT_FILENAME);
      tempCheckpointPath = checkpointPath.suffix(".tmp");
      backupCheckpointPath = checkpointPath.suffix(".bak");
      fileSystem = checkpointPath.getFileSystem(jsc.hadoopConfiguration());
      if (fileSystem.exists(checkpointPath)) {
        is = fileSystem.open(checkpointPath);
      } else if (fileSystem.exists(backupCheckpointPath)) {
        is = fileSystem.open(backupCheckpointPath);
      }
      if (is != null) {
        ObjectInputStream objectInputStream = new ObjectInputStream(is);
        NamedAggregators recoveredValue =
            (NamedAggregators) objectInputStream.readObject();
        objectInputStream.close();
        LOG.info("Recovered accumulators from checkpoint: " + recoveredValue);
        instance.setValue(recoveredValue);
      } else {
        LOG.info("No accumulator checkpoint found.");
      }
    } catch (Exception e) {
      throw new RuntimeException("Failure while reading accumulator checkpoint.", e);
    }
  }

  private static void checkpoint() throws IOException {
    if (checkpointPath != null) {
      if (fileSystem.exists(checkpointPath)) {
        if (fileSystem.exists(backupCheckpointPath)) {
          fileSystem.delete(backupCheckpointPath, false);
        }
        fileSystem.rename(checkpointPath, backupCheckpointPath);
      }
      FSDataOutputStream os = fileSystem.create(tempCheckpointPath, true);
      ObjectOutputStream oos = new ObjectOutputStream(os);
      oos.writeObject(instance.value());
      oos.close();
      fileSystem.rename(tempCheckpointPath, checkpointPath);
    }
  }

  @VisibleForTesting
  static void clear() {
    synchronized (AccumulatorSingleton.class) {
      instance = null;
    }
  }

  /**
   * Spark Listener which checkpoints {@link NamedAggregators} values for fault-tolerance.
   */
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
