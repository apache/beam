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

package org.apache.beam.runners.spark.translation.streaming;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Spark checkpoint dir tree.
 */
public class CheckpointDir {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointDir.class);

  private static final String SPARK_CHECKPOINT_DIR = "spark-checkpoint";
  private static final String KNOWN_RELIABLE_FS_PATTERN = "^(hdfs|s3|gs)";

  private final Path rootCheckpointDir;
  private final Path sparkCheckpointDir;

  public CheckpointDir(String rootCheckpointDir) {
    if (!rootCheckpointDir.matches(KNOWN_RELIABLE_FS_PATTERN)) {
      LOG.warn("The specified checkpoint dir {} does not match a reliable filesystem so in case "
          + "of failures this job may not recover properly or even at all.", rootCheckpointDir);
    }
    LOG.info("Checkpoint dir set to: {}", rootCheckpointDir);

    this.rootCheckpointDir = new Path(rootCheckpointDir);
    this.sparkCheckpointDir = new Path(rootCheckpointDir, SPARK_CHECKPOINT_DIR);
  }

  public Path getRootCheckpointDir() {
    return rootCheckpointDir;
  }

  public Path getSparkCheckpointDir() {
    return sparkCheckpointDir;
  }
}
