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

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a valid {@link InputFormat} for reading employee data, available in the form of {@code
 * List<KV>}. Used to test the {@link CdapIO#read()}.
 */
public class EmployeeInputFormat extends InputFormat<String, String> {

  public static final int NUM_OF_TEST_EMPLOYEE_RECORDS = 1000;
  public static final String EMPLOYEE_NAME_PREFIX = "Employee ";

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    return Collections.singletonList(new EmployeeSplit());
  }

  @Override
  public RecordReader<String, String> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new RecordReader<String, String>() {

      private long currentObjectId = 0L;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {}

      @Override
      public boolean nextKeyValue() {
        currentObjectId++;
        return currentObjectId < NUM_OF_TEST_EMPLOYEE_RECORDS;
      }

      @Override
      public String getCurrentKey() {
        return String.valueOf(currentObjectId);
      }

      @Override
      public String getCurrentValue() {
        return EMPLOYEE_NAME_PREFIX + currentObjectId;
      }

      @Override
      public float getProgress() {
        return 0;
      }

      @Override
      public void close() {}
    };
  }

  private static class EmployeeSplit extends InputSplit implements Writable {
    public EmployeeSplit() {}

    @Override
    public void readFields(DataInput dataInput) {}

    @Override
    public void write(DataOutput dataOutput) {}

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }
  }
}
