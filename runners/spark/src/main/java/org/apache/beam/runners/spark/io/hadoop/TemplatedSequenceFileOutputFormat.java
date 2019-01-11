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

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Templated sequence file output format.
 */
public class TemplatedSequenceFileOutputFormat<K, V> extends SequenceFileOutputFormat<K, V>
    implements ShardNameTemplateAware {

  @Override
  public void checkOutputSpecs(JobContext job) {
    // don't fail if the output already exists
  }

  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context,
      String extension) throws IOException {
    // note that the passed-in extension is ignored since it comes from the template
    return ShardNameTemplateHelper.getDefaultWorkFile(this, context);
  }

}
