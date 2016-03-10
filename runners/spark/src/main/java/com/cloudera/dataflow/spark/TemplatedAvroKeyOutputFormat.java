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
import java.io.OutputStream;

import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TemplatedAvroKeyOutputFormat<T> extends AvroKeyOutputFormat<T>
    implements ShardNameTemplateAware {

  @Override
  public void checkOutputSpecs(JobContext job) {
    // don't fail if the output already exists
  }

  @Override
  protected OutputStream getAvroFileOutputStream(TaskAttemptContext context) throws IOException {
    Path path = ShardNameTemplateHelper.getDefaultWorkFile(this, context);
    return path.getFileSystem(context.getConfiguration()).create(path);
  }

}
