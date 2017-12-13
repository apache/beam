/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.time.Clock;
import java.util.Random;

public class HadoopUtils {

  private static final JobID JOB_ID = new JobID(
      Long.toString(Clock.systemDefaultZone().millis()),
      new Random().nextInt(Integer.MAX_VALUE));

  /**
   * JobID is unique per JVM. It should be broadcasted by the driver
   * so it is consistent across executors.
   *
   * @return job id
   */
  public static JobID getJobID() {
    return JOB_ID;
  }

  public static TaskAttemptContext createSetupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_SETUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  public static TaskAttemptContext createCleanupTaskContext(Configuration conf, JobID jobID) {
    final TaskID taskId = new TaskID(jobID, TaskType.JOB_CLEANUP, 0);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }

  public static TaskAttemptContext createTaskContext(Configuration conf, JobID jobID, int taskNumber) {
    final TaskID taskId = new TaskID(jobID, TaskType.REDUCE, taskNumber);
    return new TaskAttemptContextImpl(conf, new TaskAttemptID(taskId, 0));
  }
}
