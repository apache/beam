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

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ExternalSynchronization} which registers locks in the HDFS.
 *
 * <p>Requires {@code locksDir} to be specified. This directory MUST be different that directory
 * which is possibly stored under {@code "mapreduce.output.fileoutputformat.outputdir"} key.
 * Otherwise setup of job will fail because the directory will exist before job setup.
 */
public class HDFSSynchronization implements ExternalSynchronization {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSSynchronization.class);

  private static final String LOCKS_DIR_PATTERN = "%s/";
  private static final String LOCKS_DIR_TASK_PATTERN = LOCKS_DIR_PATTERN + "%s";
  private static final String LOCKS_DIR_TASK_ATTEMPT_PATTERN = LOCKS_DIR_TASK_PATTERN + "_%s";
  private static final String LOCKS_DIR_JOB_FILENAME = LOCKS_DIR_PATTERN + "_job";

  private static final transient Random RANDOM_GEN = new Random();

  private final String locksDir;
  private final ThrowingFunction<Configuration, FileSystem, IOException> fileSystemFactory;

  /**
   * Creates instance of {@link HDFSSynchronization}.
   *
   * @param locksDir directory where locks will be stored. This directory MUST be different that
   *     directory which is possibly stored under {@code
   *     "mapreduce.output.fileoutputformat.outputdir"} key. Otherwise setup of job will fail
   *     because the directory will exist before job setup.
   */
  public HDFSSynchronization(String locksDir) {
    this(locksDir, FileSystem::newInstance);
  }

  /**
   * Creates instance of {@link HDFSSynchronization}. Exists only for easier testing.
   *
   * @param locksDir directory where locks will be stored. This directory MUST be different that
   *     directory which is possibly stored under {@code
   *     "mapreduce.output.fileoutputformat.outputdir"} key. Otherwise setup of job will fail
   *     because the directory will exist before job setup.
   * @param fileSystemFactory supplier of the file system
   */
  HDFSSynchronization(
      String locksDir, ThrowingFunction<Configuration, FileSystem, IOException> fileSystemFactory) {
    this.locksDir = locksDir;
    this.fileSystemFactory = fileSystemFactory;
  }

  @Override
  public boolean tryAcquireJobLock(Configuration conf) {
    Path path = new Path(locksDir, String.format(LOCKS_DIR_JOB_FILENAME, getJobJtIdentifier(conf)));

    return tryCreateFile(conf, path);
  }

  @Override
  public void releaseJobIdLock(Configuration conf) {
    Path path = new Path(locksDir, String.format(LOCKS_DIR_PATTERN, getJobJtIdentifier(conf)));

    try (FileSystem fileSystem = fileSystemFactory.apply(conf)) {
      if (fileSystem.delete(path, true)) {
        LOG.info("Delete of lock directory {} was successful", path);
      } else {
        LOG.warn("Delete of lock directory {} was unsuccessful", path);
      }

    } catch (IOException e) {
      String formattedExceptionMessage =
          String.format("Delete of lock directory %s was unsuccessful", path);
      LOG.warn(formattedExceptionMessage, e);
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
    try (FileSystem fileSystem = fileSystemFactory.apply(conf)) {
      try {
        return fileSystem.createNewFile(path);
      } catch (FileAlreadyExistsException | org.apache.hadoop.fs.FileAlreadyExistsException e) {
        return false;
      } catch (RemoteException e) {
        // remote hdfs exception
        if (e.getClassName().equals(AlreadyBeingCreatedException.class.getName())) {
          return false;
        }
        throw e;
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

  /**
   * Function which can throw exception.
   *
   * @param <T1> parameter type
   * @param <T2> result type
   * @param <X> exception type
   */
  @FunctionalInterface
  interface ThrowingFunction<T1, T2, X extends Exception> extends Serializable {
    T2 apply(T1 value) throws X;
  }
}
