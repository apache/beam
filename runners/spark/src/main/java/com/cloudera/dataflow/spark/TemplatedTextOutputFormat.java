/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import static com.cloudera.dataflow.spark.ShardNameBuilder.replaceShardNumber;

public class TemplatedTextOutputFormat<K, V> extends TextOutputFormat<K, V> {

  public static final String OUTPUT_FILE_PREFIX = "spark.dataflow.textoutputformat.prefix";
  public static final String OUTPUT_FILE_TEMPLATE = "spark.dataflow.textoutputformat.template";
  public static final String OUTPUT_FILE_SUFFIX = "spark.dataflow.textoutputformat.suffix";

  @Override
  public void checkOutputSpecs(JobContext job) {
    // don't fail if the output already exists
  }

  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context,
      String extension) throws IOException {
    // note that the passed-in extension is ignored since it comes from the template
    FileOutputCommitter committer =
        (FileOutputCommitter) getOutputCommitter(context);
    return new Path(committer.getWorkPath(), getOutputFile(context));
  }

  private String getOutputFile(TaskAttemptContext context) {
    TaskID taskId = context.getTaskAttemptID().getTaskID();
    int partition = taskId.getId();

    String filePrefix = context.getConfiguration().get(OUTPUT_FILE_PREFIX);
    String fileTemplate = context.getConfiguration().get(OUTPUT_FILE_TEMPLATE);
    String fileSuffix = context.getConfiguration().get(OUTPUT_FILE_SUFFIX);
    return filePrefix + replaceShardNumber(fileTemplate, partition) + fileSuffix;
  }
}
