/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Bad InputFormat returns list of InputSplits containing null values.
 */
public class BadGetSplitsInputFormat extends InputFormat<Text, Employee> {

  public BadGetSplitsInputFormat() {}

  @Override
  public RecordReader<Text, Employee> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new BadGetSplitsRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    // InputSplit list having null value.
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    inputSplitList.add(new BadGetSplitsInputSplit());
    inputSplitList.add(null);
    return inputSplitList;
  }

  public class BadGetSplitsInputSplit extends InputSplit implements Writable {

    @Override
    public void readFields(DataInput in) throws IOException {}

    @Override
    public void write(DataOutput out) throws IOException {}

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return null;
    }
  }

  public class BadGetSplitsRecordReader extends RecordReader<Text, Employee> {

    @Override
    public void close() throws IOException {}

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return new Text();
    }

    @Override
    public Employee getCurrentValue() throws IOException, InterruptedException {
      return new Employee();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {}

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return false;
    }
  }
}
