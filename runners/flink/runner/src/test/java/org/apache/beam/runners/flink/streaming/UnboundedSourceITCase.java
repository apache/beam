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
package org.apache.beam.runners.flink.streaming;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.joda.time.Instant;
import org.junit.internal.ArrayComparisonFailure;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


public class UnboundedSourceITCase extends StreamingProgramTestBase {

  protected static String resultPath;

  public UnboundedSourceITCase() {
  }

  static final String[] EXPECTED_RESULT = new String[]{
      "1", "2", "3", "4", "5", "6", "7", "8", "9"};

  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
  }

  @Override
  protected void testProgram() throws Exception {
    runProgram(resultPath);
  }

  private static void runProgram(String resultPath) {

    Pipeline p = FlinkTestPipeline.createForStreaming();

    PCollection<String> result = p
        .apply(Read.from(new RangeReadSource(1, 10)))
        .apply(Window.<Integer>into(new GlobalWindows())
            .triggering(AfterPane.elementCountAtLeast(10))
            .discardingFiredPanes())
        .apply(ParDo.of(new DoFn<Integer, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            c.output(c.element().toString());
          }
        }));

    result.apply(TextIO.Write.to(resultPath));

    try {
      p.run();
      fail();
    } catch(Exception e) {
      assertEquals("The source terminates as expected.", e.getCause().getCause().getMessage());
    }
  }


  private static class RangeReadSource extends UnboundedSource<Integer, UnboundedSource.CheckpointMark> {

    final int from;
    final int to;

    RangeReadSource(int from, int to) {
      this.from = from;
      this.to = to;
    }


    @Override
    public List<? extends UnboundedSource<Integer, CheckpointMark>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public UnboundedReader<Integer> createReader(PipelineOptions options, @Nullable CheckpointMark checkpointMark) {
      return new RangeReadReader(options);
    }

    @Nullable
    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public void validate() {
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return BigEndianIntegerCoder.of();
    }

    private class RangeReadReader extends UnboundedReader<Integer> {

      private int current;

      private long watermark;

      public RangeReadReader(PipelineOptions options) {
        assertNotNull(options);
        current = from;
      }

      @Override
      public boolean start() throws IOException {
        return true;
      }

      @Override
      public boolean advance() throws IOException {
        current++;
        watermark++;

        if (current >= to) {
          try {
            compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
            throw new IOException("The source terminates as expected.");
          } catch (IOException e) {
            // pass on the exception to terminate the source
            throw e;
          } catch (Throwable t) {
            // expected here from the file check
          }
        }
        return current < to;
      }

      @Override
      public Integer getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return new Instant(current);
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public Instant getWatermark() {
        return new Instant(watermark);
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return null;
      }

      @Override
      public UnboundedSource<Integer, ?> getCurrentSource() {
        return RangeReadSource.this;
      }
    }
  }
}


