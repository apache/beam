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
package org.apache.beam.sdk.io.hadoop.format;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 * This is a dummy input format to test reading using HadoopFormatIO.Read if InputFormat implements
 * Configurable. This validates if setConf() method is called before getSplits(). Known InputFormats
 * which implement Configurable are DBInputFormat, TableInputFormat etc.
 */
class ConfigurableEmployeeInputFormat extends InputFormat<Text, Employee> implements Configurable {
  public boolean isConfSet = false;

  public ConfigurableEmployeeInputFormat() {}

  @Override
  public Configuration getConf() {
    return null;
  }

  /** Set configuration properties such as number of splits and number of records in each split. */
  @Override
  public void setConf(Configuration conf) {
    isConfSet = true;
  }

  @Override
  public RecordReader<Text, Employee> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new ConfigurableEmployeeRecordReader();
  }

  /**
   * Returns InputSPlit list of {@link ConfigurableEmployeeInputSplit}. Throws exception if {@link
   * #setConf(Configuration)} is not called.
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    if (!isConfSet) {
      throw new IOException("Configuration is not set.");
    }
    List<InputSplit> splits = new ArrayList<>();
    splits.add(new ConfigurableEmployeeInputSplit());
    return splits;
  }

  /** InputSplit implementation for ConfigurableEmployeeInputFormat. */
  static class ConfigurableEmployeeInputSplit extends InputSplit implements Writable {

    @Override
    public void readFields(DataInput arg0) {}

    @Override
    public void write(DataOutput arg0) {}

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return null;
    }
  }

  /** RecordReader for ConfigurableEmployeeInputFormat. */
  static class ConfigurableEmployeeRecordReader extends RecordReader<Text, Employee> {

    @Override
    public void initialize(
        InputSplit paramInputSplit, TaskAttemptContext paramTaskAttemptContext) {}

    @Override
    public boolean nextKeyValue() {
      return false;
    }

    @Override
    public Text getCurrentKey() {
      return null;
    }

    @Override
    public Employee getCurrentValue() {
      return null;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void close() {}
  }
}
