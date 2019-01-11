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

package org.apache.beam.runners.spark.io.hadoop;

import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardNumber;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shard name template helper.
 */
public final class ShardNameTemplateHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ShardNameTemplateHelper.class);

  public static final String OUTPUT_FILE_PREFIX = "spark.beam.fileoutputformat.prefix";
  public static final String OUTPUT_FILE_TEMPLATE = "spark.beam.fileoutputformat.template";
  public static final String OUTPUT_FILE_SUFFIX = "spark.beam.fileoutputformat.suffix";

  private ShardNameTemplateHelper() {
  }

  public static <K, V> Path getDefaultWorkFile(FileOutputFormat<K, V> format,
      TaskAttemptContext context) throws IOException {
    FileOutputCommitter committer =
        (FileOutputCommitter) format.getOutputCommitter(context);
    return new Path(committer.getWorkPath(), getOutputFile(context));
  }

  private static String getOutputFile(TaskAttemptContext context) {
    TaskID taskId = context.getTaskAttemptID().getTaskID();
    int partition = taskId.getId();

    String filePrefix = context.getConfiguration().get(OUTPUT_FILE_PREFIX);
    String fileTemplate = context.getConfiguration().get(OUTPUT_FILE_TEMPLATE);
    String fileSuffix = context.getConfiguration().get(OUTPUT_FILE_SUFFIX);
    return filePrefix + replaceShardNumber(fileTemplate, partition) + fileSuffix;
  }

}
