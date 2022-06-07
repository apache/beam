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
package org.apache.beam.sdk.io.cdap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

public class EmployeeInputFormat extends InputFormat<String, String> {

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    return Collections.singletonList(new EmployeeSplit());
  }

  @Override
  public RecordReader<String, String> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new RecordReader<String, String>() {

      private Pair<String, String> currentObject;
      private long currentObjectId = 0L;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {}

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        currentObject =
            new ImmutablePair<>(String.valueOf(currentObjectId), "Employee " + currentObjectId);
        currentObjectId++;
        return currentObjectId < 1000;
      }

      @Override
      public String getCurrentKey() throws IOException, InterruptedException {
        return currentObject.getKey();
      }

      @Override
      public String getCurrentValue() throws IOException, InterruptedException {
        return currentObject.getValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override
      public void close() throws IOException {}
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
