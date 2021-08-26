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

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

/**
 * Provides mechanism for acquiring locks related to the job. Serves as source of unique events
 * among the job.
 */
public interface ExternalSynchronization extends Serializable {

  /**
   * Tries to acquire lock for given job.
   *
   * @param conf configuration bounded with given job.
   * @return {@code true} if the lock was acquired, {@code false} otherwise.
   */
  boolean tryAcquireJobLock(Configuration conf);

  /**
   * Deletes lock ids bounded with given job if any exists.
   *
   * @param conf hadoop configuration of given job.
   */
  void releaseJobIdLock(Configuration conf);

  /**
   * Creates {@link TaskID} with unique id among given job.
   *
   * @param conf hadoop configuration of given job.
   * @return {@link TaskID} with unique id among given job.
   */
  TaskID acquireTaskIdLock(Configuration conf);

  /**
   * Creates unique {@link TaskAttemptID} for given taskId.
   *
   * @param conf configuration of given task and job
   * @param taskId id of the task
   * @return Unique {@link TaskAttemptID} for given taskId.
   */
  TaskAttemptID acquireTaskAttemptIdLock(Configuration conf, int taskId);
}
