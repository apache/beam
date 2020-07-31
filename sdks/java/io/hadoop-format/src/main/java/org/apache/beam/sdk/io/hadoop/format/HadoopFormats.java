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

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility class for working with Hadoop related objects. */
final class HadoopFormats {

  private static final int DEFAULT_JOB_NUMBER = 0;
  static final Class<HashPartitioner> DEFAULT_PARTITIONER_CLASS_ATTR = HashPartitioner.class;
  private static final int DEFAULT_NUM_REDUCERS = 1;

  private HadoopFormats() {}

  /**
   * Creates {@link JobID} with random jtIdentifier and default job number.
   *
   * @return new {@link JobID}
   */
  public static JobID createJobId() {
    return new JobID(UUID.randomUUID().toString(), DEFAULT_JOB_NUMBER);
  }

  /**
   * Creates {@link JobID} with specified jtIdentifier and default job number.
   *
   * @param jtIdentifier jtIdentifier to specify
   * @return new {@link JobID}
   */
  public static JobID createJobId(String jtIdentifier) {
    return new JobID(jtIdentifier, DEFAULT_JOB_NUMBER);
  }

  /**
   * Creates new setup {@link TaskAttemptContext} from hadoop {@link Configuration} and {@link
   * JobID}.
   *
   * @param conf hadoop {@link Configuration}
   * @param jobID jobId of the created {@link TaskAttemptContext}
   * @return new setup {@link TaskAttemptContext}
   */
  static TaskAttemptContext createSetupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_SETUP, 0);
    return createTaskAttemptContext(conf, new TaskAttemptID(taskId, 0));
  }

  /**
   * Creates new {@link TaskAttemptContext} from hadoop {@link Configuration}, {@link JobID} and
   * specified taskNumber.
   *
   * @param conf hadoop {@link Configuration}
   * @param jobID jobId of the created {@link TaskAttemptContext}
   * @param taskNumber number of the task (should be unique across one job)
   * @return new {@link TaskAttemptContext}
   */
  static TaskAttemptContext createTaskAttemptContext(
      Configuration conf, JobID jobID, int taskNumber) {
    TaskAttemptID taskAttemptID = createTaskAttemptID(jobID, taskNumber, 0);
    return createTaskAttemptContext(conf, taskAttemptID);
  }

  /**
   * Creates {@link TaskAttemptContext}.
   *
   * @param conf configuration
   * @param taskAttemptID taskAttemptId
   * @return new {@link TaskAttemptContext}
   */
  static TaskAttemptContext createTaskAttemptContext(
      Configuration conf, TaskAttemptID taskAttemptID) {
    return new TaskAttemptContextImpl(conf, taskAttemptID);
  }

  /**
   * Creates new {@link TaskAttemptID}.
   *
   * @param jobID jobId
   * @param taskId taskId
   * @param attemptId attemptId
   * @return new {@link TaskAttemptID}
   */
  static TaskAttemptID createTaskAttemptID(JobID jobID, int taskId, int attemptId) {
    final TaskID tId = createTaskID(jobID, taskId);
    return new TaskAttemptID(tId, attemptId);
  }

  /**
   * Creates new {@link TaskID} with specified {@code taskNumber} for given {@link JobID}.
   *
   * @param jobID jobId of the created {@link TaskID}
   * @param taskNumber number of the task (should be unique across one job)
   * @return new {@link TaskID} for given {@link JobID}
   */
  static TaskID createTaskID(JobID jobID, int taskNumber) {
    return new TaskID(jobID, TaskType.REDUCE, taskNumber);
  }

  /**
   * Creates cleanup {@link TaskAttemptContext} for given {@link JobID}.
   *
   * @param conf hadoop configuration
   * @param jobID jobId of the created {@link TaskID}
   * @return new cleanup {@link TaskID} for given {@link JobID}
   */
  static TaskAttemptContext createCleanupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_CLEANUP, 0);
    return createTaskAttemptContext(conf, new TaskAttemptID(taskId, 0));
  }

  /**
   * Returns instance of {@link OutputFormat} by class name stored in the configuration under key
   * {@link MRJobConfig#OUTPUT_FORMAT_CLASS_ATTR}.
   *
   * @param conf Hadoop configuration
   * @param <KeyT> KeyType of output format
   * @param <ValueT> ValueType of output format
   * @return OutputFormatter
   * @throws IllegalArgumentException if particular key was not found in the config or Formatter was
   *     unable to construct.
   */
  @SuppressWarnings("unchecked")
  static <KeyT, ValueT> OutputFormat<KeyT, ValueT> createOutputFormatFromConfig(Configuration conf)
      throws IllegalArgumentException {
    return (OutputFormat<KeyT, ValueT>)
        createInstanceFromConfig(
            conf, MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, null, OutputFormat.class);
  }

  /**
   * Creates new instance of {@link Partitioner} by class specified in hadoop {@link Configuration}.
   *
   * @param conf hadoop Configuration
   * @param <KeyT> KeyType of {@link Partitioner}
   * @param <ValueT> ValueTYpe of {@link Partitioner}
   * @return new {@link Partitioner}
   */
  @SuppressWarnings("unchecked")
  static <KeyT, ValueT> Partitioner<KeyT, ValueT> getPartitioner(Configuration conf) {
    return (Partitioner<KeyT, ValueT>)
        createInstanceFromConfig(
            conf,
            MRJobConfig.PARTITIONER_CLASS_ATTR,
            DEFAULT_PARTITIONER_CLASS_ATTR,
            Partitioner.class);
  }

  /**
   * Creates object from class specified in the configuration under specified {@code
   * configClassKey}.
   *
   * @param conf hadoop Configuration where is stored class name of returned object
   * @param configClassKey key for class name
   * @param defaultClass Default class if any result was not found under specified {@code
   *     configClassKey}
   * @param xface interface of given class
   * @return created object
   */
  private static <T> T createInstanceFromConfig(
      Configuration conf,
      String configClassKey,
      @Nullable Class<? extends T> defaultClass,
      Class<T> xface) {
    try {
      String className = conf.get(configClassKey);
      Preconditions.checkArgument(
          className != null || defaultClass != null,
          String.format(
              "Configuration does not contains any value under %s key. Unable to initialize class instance from configuration. ",
              configClassKey));

      Class<? extends T> requiredClass = conf.getClass(configClassKey, defaultClass, xface);

      return requiredClass.getConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to create instance of object from configuration under key %s.",
              configClassKey),
          e);
    }
  }

  /**
   * Creates {@link JobID} with {@code jtIdentifier} specified in hadoop {@link Configuration} under
   * {@link MRJobConfig#ID} key.
   *
   * @param conf hadoop {@link Configuration}
   * @return JobID created from {@link Configuration}
   */
  static JobID getJobId(Configuration conf) {
    String jobJtIdentifier =
        Preconditions.checkNotNull(
            conf.get(MRJobConfig.ID),
            "Configuration must contain jobID under key \"%s\".",
            HadoopFormatIO.JOB_ID);

    return new JobID(jobJtIdentifier, DEFAULT_JOB_NUMBER);
  }

  /**
   * Returns count of the reducers specified under key {@link MRJobConfig#NUM_REDUCES} in hadoop
   * {@link Configuration}.
   *
   * @param conf hadoop {@link Configuration}
   * @return configured count of reducers
   */
  static int getReducersCount(Configuration conf) {
    return conf.getInt(MRJobConfig.NUM_REDUCES, DEFAULT_NUM_REDUCERS);
  }
}
