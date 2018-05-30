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
package org.apache.beam.runners.flink.translation.wrappers;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

/** Flink input format that implements impulses. */
public class ImpulseInputFormat extends RichInputFormat<WindowedValue<byte[]>, GenericInputSplit> {

  // Whether the input format has remaining output that has not yet been read.
  private boolean availableOutput = false;

  public ImpulseInputFormat() {}

  @Override
  public void configure(Configuration configuration) {
    // Do nothing.
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
    return new BaseStatistics() {
      @Override
      public long getTotalInputSize() {
        return 1;
      }

      @Override
      public long getNumberOfRecords() {
        return 1;
      }

      @Override
      public float getAverageRecordWidth() {
        return 1;
      }
    };
  }

  @Override
  public GenericInputSplit[] createInputSplits(int numSplits) {
    // Always return a single split because only one global "impulse" will ever be sent.
    return new GenericInputSplit[]{new GenericInputSplit(1, 1)};
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] genericInputSplits) {
    return new DefaultInputSplitAssigner(genericInputSplits);
  }

  @Override
  public void open(GenericInputSplit genericInputSplit) {
    availableOutput = true;
  }

  @Override
  public boolean reachedEnd() {
    return !availableOutput;
  }

  @Override
  public WindowedValue<byte[]> nextRecord(WindowedValue<byte[]> windowedValue) {
    checkState(availableOutput);
    availableOutput = false;
    if (windowedValue != null) {
      return windowedValue;
    }
    return WindowedValue.valueInGlobalWindow(new byte[0]);
  }

  @Override
  public void close() {
    // Do nothing.
  }

}
