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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Checkpoint data to make it available in future pipeline runs. */
public class Checkpoint {
  private static final String TEMP_FILE_SUFFIX = ".tmp";
  private static final String BACKUP_FILE_SUFFIX = ".bak";

  private static void write(FileSystem fileSystem, Path checkpointFilePath, byte[] value)
      throws IOException {
    Path tmpPath = checkpointFilePath.suffix(TEMP_FILE_SUFFIX);
    Path backupPath = checkpointFilePath.suffix(BACKUP_FILE_SUFFIX);
    if (fileSystem.exists(checkpointFilePath)) {
      if (fileSystem.exists(backupPath)) {
        fileSystem.delete(backupPath, false);
      }
      fileSystem.rename(checkpointFilePath, backupPath);
    }
    FSDataOutputStream os = fileSystem.create(tmpPath, true);
    os.write(value);
    os.close();
    fileSystem.rename(tmpPath, checkpointFilePath);
  }

  public static void writeObject(FileSystem fileSystem, Path checkpointFilePath, Object value)
      throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(value);
    oos.close();
    write(fileSystem, checkpointFilePath, bos.toByteArray());
  }

  private static byte[] read(FileSystem fileSystem, Path checkpointFilePath) throws IOException {
    Path backupCheckpointPath = checkpointFilePath.suffix(".bak");
    FSDataInputStream is = null;
    if (fileSystem.exists(checkpointFilePath)) {
      is = fileSystem.open(checkpointFilePath);
    } else if (fileSystem.exists(backupCheckpointPath)) {
      is = fileSystem.open(backupCheckpointPath);
    }
    return is != null ? IOUtils.toByteArray(is) : null;
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <T> T readObject(FileSystem fileSystem, Path checkpointfilePath)
      throws IOException, ClassNotFoundException {
    byte[] bytes = read(fileSystem, checkpointfilePath);
    if (bytes == null) {
      return null;
    }
    ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    T value = (T) objectInputStream.readObject();
    objectInputStream.close();
    return value;
  }

  /**
   * Checkpoint dir tree.
   *
   * <p>{@link SparkPipelineOptions} checkpointDir is used as a root directory under which one
   * directory is created for Spark's checkpoint and another for Beam's Spark runner's fault
   * checkpointing needs. Spark's checkpoint relies on Hadoop's {@link
   * org.apache.hadoop.fs.FileSystem} and is used for Beam as well rather than {@link
   * org.apache.beam.sdk.io.FileSystem} to be consistent with Spark.
   */
  public static class CheckpointDir {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointDir.class);

    private static final String SPARK_CHECKPOINT_DIR = "spark-checkpoint";
    private static final String BEAM_CHECKPOINT_DIR = "beam-checkpoint";
    private static final String KNOWN_RELIABLE_FS_PATTERN = "^(hdfs|s3|gs)";

    private final Path rootCheckpointDir;
    private final Path sparkCheckpointDir;
    private final Path beamCheckpointDir;

    public CheckpointDir(String rootCheckpointDir) {
      if (!rootCheckpointDir.matches(KNOWN_RELIABLE_FS_PATTERN)) {
        LOG.warn(
            "The specified checkpoint dir {} does not match a reliable filesystem so in case "
                + "of failures this job may not recover properly or even at all.",
            rootCheckpointDir);
      }
      LOG.info("Checkpoint dir set to: {}", rootCheckpointDir);

      this.rootCheckpointDir = new Path(rootCheckpointDir);
      this.sparkCheckpointDir = new Path(rootCheckpointDir, SPARK_CHECKPOINT_DIR);
      this.beamCheckpointDir = new Path(rootCheckpointDir, BEAM_CHECKPOINT_DIR);
    }

    public Path getRootCheckpointDir() {
      return rootCheckpointDir;
    }

    public Path getSparkCheckpointDir() {
      return sparkCheckpointDir;
    }

    public Path getBeamCheckpointDir() {
      return beamCheckpointDir;
    }
  }
}
