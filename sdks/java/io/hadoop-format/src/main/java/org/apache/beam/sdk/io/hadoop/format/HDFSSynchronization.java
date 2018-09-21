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
package org.apache.beam.sdk.io.hadoop.format;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ExternalSynchronization} which registers locks in the HDFS. Requires
 *
 * <p>{@code locksDir} to be specified. This directory MUST be different that directory which is
 * possibly stored under {@code "mapreduce.output.fileoutputformat.outputdir"} key. Otherwise setup
 * of job will fail because the directory will exist before job setup.
 */
public class HDFSSynchronization implements ExternalSynchronization {

  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSSynchronization.class);

  private static final String LOCKS_DIR_PATTERN = "%s/";
  private static final String LOCKS_DIR_TASK_PATTERN = LOCKS_DIR_PATTERN + "%s";
  private static final String LOCKS_DIR_TASK_ATTEMPT_PATTERN = LOCKS_DIR_TASK_PATTERN + "_%s";
  private static final String LOCKS_DIR_JOB_FILENAME = LOCKS_DIR_PATTERN + "_job";

  private static final transient Random RANDOM_GEN = new Random();

  private final String locksDir;

  /**
   * Creates instance of {@link HDFSSynchronization}.
   *
   * @param locksDir directory where locks will be stored. This directory MUST be different that
   *     directory which is possibly stored under {@code
   *     "mapreduce.output.fileoutputformat.outputdir"} key. Otherwise setup of job will fail
   *     because the directory will exist before job setup.
   */
  public HDFSSynchronization(String locksDir) {
    this.locksDir = locksDir;
  }

  @Override
  public boolean tryAcquireJobLock(Configuration conf) {
    Path path = new Path(locksDir, String.format(LOCKS_DIR_JOB_FILENAME, getJobJtIdentifier(conf)));

    return tryCreateFile(conf, path);
  }

  @Override
  public void releaseJobIdLock(Configuration conf) {
    Path path = new Path(locksDir, String.format(LOCKS_DIR_PATTERN, getJobJtIdentifier(conf)));

    try {
      if (FileSystem.get(conf).delete(path, true)) {
        LOGGER.info("Delete of lock directory {} was successful", path);
      } else {
        LOGGER.warn("Delete of lock directory {} was unsuccessful", path);
      }

    } catch (IOException e) {
      String formattedExceptionMessage =
          String.format("Delete of lock directory %s was unsuccessful", path);
      LOGGER.warn(formattedExceptionMessage, e);
      throw new IllegalStateException(formattedExceptionMessage, e);
    }
  }

  @Override
  public TaskID acquireTaskIdLock(Configuration conf) {
    JobID jobId = HadoopFormats.getJobId(conf);
    boolean lockAcquired = false;
    int taskIdCandidate = 0;

    while (!lockAcquired) {
      taskIdCandidate = RANDOM_GEN.nextInt(Integer.MAX_VALUE);
      Path path =
          new Path(
              locksDir,
              String.format(LOCKS_DIR_TASK_PATTERN, getJobJtIdentifier(conf), taskIdCandidate));
      lockAcquired = tryCreateFile(conf, path);
    }

    return HadoopFormats.createTaskID(jobId, taskIdCandidate);
  }

  @Override
  public TaskAttemptID acquireTaskAttemptIdLock(Configuration conf, int taskId) {
    String jobJtIdentifier = getJobJtIdentifier(conf);
    JobID jobId = HadoopFormats.getJobId(conf);
    int taskAttemptCandidate = 0;
    boolean taskAttemptAcquired = false;

    while (!taskAttemptAcquired) {
      taskAttemptCandidate++;
      Path path =
          new Path(
              locksDir,
              String.format(
                  LOCKS_DIR_TASK_ATTEMPT_PATTERN, jobJtIdentifier, taskId, taskAttemptCandidate));
      taskAttemptAcquired = tryCreateFile(conf, path);
    }

    return HadoopFormats.createTaskAttemptID(jobId, taskId, taskAttemptCandidate);
  }

  private boolean tryCreateFile(Configuration conf, Path path) {
    try {
      FileSystem fileSystem = FileSystem.get(conf);

      try {
        return fileSystem.createNewFile(path);
      } catch (FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException e) {
        return false;
      }

    } catch (IOException e) {
      throw new IllegalStateException(String.format("Creation of file on path %s failed", path), e);
    }
  }

  private String getJobJtIdentifier(Configuration conf) {
    JobID job =
        Preconditions.checkNotNull(
            HadoopFormats.getJobId(conf),
            "Configuration must contain jobID under key %s.",
            HadoopFormatIO.JOB_ID);
    return job.getJtIdentifier();
  }
}
