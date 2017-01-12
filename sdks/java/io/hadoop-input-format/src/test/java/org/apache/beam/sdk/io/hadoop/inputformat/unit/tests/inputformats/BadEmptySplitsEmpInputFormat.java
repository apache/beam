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
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *  Bad Employee input format which returns empty list of input splits in getSplits() method
 */
public class BadEmptySplitsEmpInputFormat extends InputFormat<Text, Employee> {

  public BadEmptySplitsEmpInputFormat() {}

  @Override
  public RecordReader<Text, Employee> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new EmptyInputSplitsRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
    return new ArrayList<InputSplit>(); //return empty splits
  }

  public class EmptyInputSplitsInputSplit extends InputSplit implements Writable {
    private long startIndex;
    private long endIndex;

    public EmptyInputSplitsInputSplit() {}

    public EmptyInputSplitsInputSplit(long startIndex, long endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    /**
     *  Returns number of records in each split.
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
      return this.endIndex - this.startIndex;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return null;
    }

    public long getStartIndex() {
      return startIndex;
    }

    public long getEndIndex() {
      return endIndex;
    }


    @Override
    public void readFields(DataInput dataIn) throws IOException {
      startIndex = dataIn.readLong();
      endIndex = dataIn.readLong();
    }

    @Override
    public void write(DataOutput dataOut) throws IOException {
      dataOut.writeLong(startIndex);
      dataOut.writeLong(endIndex);
    }

  }

  class EmptyInputSplitsRecordReader extends RecordReader<Text, Employee> {

    private EmptyInputSplitsInputSplit split;
    private Text currentKey;
    private Employee currentValue;
    private long employeeMapIndex = 0L;
    private long recordsRead = 0L;
    private Map<Long, Employee> employeeData = new HashMap<Long, Employee>();

    public EmptyInputSplitsRecordReader() {}

    @Override
    public void close() throws IOException {}

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public Employee getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return (float) recordsRead / split.getLength();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
      this.split = (EmptyInputSplitsInputSplit) split;
      employeeMapIndex = this.split.getStartIndex() - 1;
      recordsRead = 0;
      populateEmployeeData();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if ((recordsRead++) == split.getLength())
        return false;
      employeeMapIndex++;
      boolean hasNext = employeeData.containsKey(employeeMapIndex);
      if (hasNext) {
        currentKey = new Text(String.valueOf(employeeMapIndex));
        currentValue = employeeData.get(employeeMapIndex);
      }
      return hasNext;
    }

    private void populateEmployeeData() {
      employeeData.put(0L, new Employee("Alex", "US"));
      employeeData.put(1L, new Employee("John", "UK"));
      employeeData.put(2L, new Employee("Tom", "UK"));
      employeeData.put(3L, new Employee("Nick", "UAE"));
      employeeData.put(4L, new Employee("Smith", "IND"));
      employeeData.put(5L, new Employee("Taylor", "US"));
      employeeData.put(6L, new Employee("Gray", "UK"));
      employeeData.put(7L, new Employee("James", "UAE"));
      employeeData.put(8L, new Employee("Jordan", "IND"));
    }
  }
}
