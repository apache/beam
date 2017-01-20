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

import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>
 * This is a InputFormat for reading employee data which is available in the form of
 * {@code List<KV>} as {@linkplain ConfigurableEmployeeRecordReader#employeeDataList
 * employeeDataList}. {@linkplain ConfigurableEmployeeRecordReader#employeeDataList
 * employeeDataList} is populated using {@linkplain TestEmployeeDataSet#populateEmployeeData()}.
 * <p>
 * This is input to test reading using HadoopInputFormatIO if InputFormat implements Configurable.
 * Known InputFormats which implements Configurable are DBInputFormat, TableInputFormat etc.
 */
public class ConfigurableEmployeeInputFormat extends InputFormat<Text, Employee>
    implements Configurable {
  private long NUMBER_OF_SPLITS;
  private long NUMBER_OF_RECORDS_IN_EACH_SPLIT;

  public ConfigurableEmployeeInputFormat() {}

  @Override
  public Configuration getConf() {
    return null;
  }

  /**
   * Set configuration properties such as number of splits and number of records in each split.
   */
  @Override
  public void setConf(Configuration conf) {
    NUMBER_OF_SPLITS = conf.getLong("split.count", 3L);
    NUMBER_OF_RECORDS_IN_EACH_SPLIT = conf.getLong("split.records.count", 5L);
  }

  @Override
  public RecordReader<Text, Employee> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new ConfigurableEmployeeRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException, InterruptedException {
    List<InputSplit> inputSplitList = new ArrayList<InputSplit>();
    for (int i = 1; i <= NUMBER_OF_SPLITS; i++) {
      InputSplit inputSplitObj = new ConfigurableEmployeeInputSplit(
          ((i - 1) * NUMBER_OF_RECORDS_IN_EACH_SPLIT), (i * NUMBER_OF_RECORDS_IN_EACH_SPLIT - 1));
      inputSplitList.add(inputSplitObj);
    }
    return inputSplitList;
  }

  public class ConfigurableEmployeeInputSplit extends InputSplit implements Writable {
    // Start and end map index of each split of employeeData.
    private long startIndex;
    private long endIndex;

    public ConfigurableEmployeeInputSplit() {}

    public ConfigurableEmployeeInputSplit(long startIndex, long endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    /**
     * Returns number of records in each split.
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
      return this.endIndex - this.startIndex + 1;
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

  public class ConfigurableEmployeeRecordReader extends RecordReader<Text, Employee> {

    private ConfigurableEmployeeInputSplit split;
    private Text currentKey;
    private Employee currentValue;
    private long employeeListIndex = 0L;
    private long recordsRead = 0L;
    private List<KV<String, String>> employeeDataList;

    public ConfigurableEmployeeRecordReader() {}

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
      this.split = (ConfigurableEmployeeInputSplit) split;
      employeeListIndex = this.split.getStartIndex() - 1;
      recordsRead = 0;
      employeeDataList = TestEmployeeDataSet.populateEmployeeData();
      currentValue = new Employee(null, null);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if ((recordsRead++) >= split.getLength()) {
        return false;
      }
      employeeListIndex++;
      KV<String, String> employeeDetails = employeeDataList.get((int) employeeListIndex);
      String empData[] = employeeDetails.getValue().split("_");
      currentKey = new Text(employeeDetails.getKey());
      currentValue = new Employee(empData[0], empData[1]);
      return true;
    }
  }
}
