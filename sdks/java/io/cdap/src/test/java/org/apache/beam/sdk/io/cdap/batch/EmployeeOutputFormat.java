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
package org.apache.beam.sdk.io.cdap.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a valid {@link OutputFormat} for writing employee data, available in the form of {@code
 * List<KV>}. Used to test the {@link CdapIO#write()}.
 */
public class EmployeeOutputFormat extends OutputFormat<String, String> {
  private static volatile List<KV<String, String>> output;
  private static OutputCommitter outputCommitter;

  @Override
  public RecordWriter<String, String> getRecordWriter(TaskAttemptContext context) {
    return new RecordWriter<String, String>() {
      @Override
      public void write(String key, String value) {
        output.add(KV.of(key, value));
      }

      @Override
      public void close(TaskAttemptContext context) {}
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context) {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return outputCommitter;
  }

  public static synchronized void initWrittenOutput(OutputCommitter outputCommitter) {
    EmployeeOutputFormat.outputCommitter = outputCommitter;
    output = Collections.synchronizedList(new ArrayList<>());
  }

  public static List<KV<String, String>> getWrittenOutput() {
    return output;
  }

  public static OutputCommitter getOutputCommitter() {
    return outputCommitter;
  }
}
