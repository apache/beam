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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** Common {@link OutputReceiver} and {@link MultiOutputReceiver} classes. */
@Internal
public class DoFnOutputReceivers {
  public interface OutputInterface<DefaultOutputT> {
    void output(DefaultOutputT output);

    <T> void output(TupleTag<T> tag, T output);

    void outputWithTimestamp(DefaultOutputT output, Instant timestamp);

    <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp);
  }

  public static <DefaultOutputT> OutputInterface fromWindowedContext(
      DoFn<?, DefaultOutputT>.WindowedContext context) {
    return new OutputInterface<DefaultOutputT>() {
      @Override
      public void output(DefaultOutputT output) {
        context.output(output);
      }

      @Override
      public <T> void output(TupleTag<T> tag, T output) {
        context.output(tag, output);
      }

      @Override
      public void outputWithTimestamp(DefaultOutputT output, Instant timestamp) {
        context.outputWithTimestamp(output, timestamp);
      }

      @Override
      public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        context.outputWithTimestamp(tag, output, timestamp);
      }
    };
  }

  private static class RowOutputReceiver<T> implements OutputReceiver<Row> {
    WindowedOutputReceiver<T> outputReceiver;
    SchemaCoder<T> schemaCoder;

    public RowOutputReceiver(
        OutputInterface<T> outputInterface,
        @Nullable TupleTag<T> outputTag,
        SchemaCoder<T> schemaCoder) {
      outputReceiver = new WindowedOutputReceiver<>(outputInterface, outputTag);
      this.schemaCoder = checkNotNull(schemaCoder);
    }

    @Override
    public void output(Row output) {
      outputReceiver.output(schemaCoder.getFromRowFunction().apply(output));
    }

    @Override
    public void outputWithTimestamp(Row output, Instant timestamp) {
      outputReceiver.outputWithTimestamp(schemaCoder.getFromRowFunction().apply(output), timestamp);
    }
  }

  private static class WindowedOutputReceiver<T> implements OutputReceiver<T> {
    final OutputInterface<T> output;
    final @Nullable TupleTag<T> outputTag;

    public WindowedOutputReceiver(OutputInterface<T> output, @Nullable TupleTag<T> outputTag) {
      this.output = output;
      this.outputTag = outputTag;
    }

    @Override
    public void output(T element) {
      if (outputTag != null) {
        output.output(outputTag, element);
      } else {
        output.output(element);
      }
    }

    @Override
    public void outputWithTimestamp(T element, Instant timestamp) {
      if (outputTag != null) {
        output.outputWithTimestamp(outputTag, element, timestamp);
      } else {
        output.outputWithTimestamp(element, timestamp);
      }
    }
  }

  private static class WindowedMultiOutputReceiver implements MultiOutputReceiver {
    OutputInterface<?> output;
    @Nullable Map<TupleTag<?>, Coder<?>> outputCoders;

    public WindowedMultiOutputReceiver(
        OutputInterface<?> output, @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
      this.output = output;
      this.outputCoders = outputCoders;
    }

    @Override
    public <T> OutputReceiver<T> get(TupleTag<T> tag) {
      return DoFnOutputReceivers.windowedReceiver((OutputInterface<T>) output, tag);
    }

    @Override
    public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
      Coder<T> outputCoder = (Coder<T>) checkNotNull(outputCoders).get(tag);
      checkState(outputCoder != null, "No output tag for " + tag);
      checkState(
          outputCoder instanceof SchemaCoder,
          "Output with tag " + tag + " must have a schema in order to call " + " getRowReceiver");
      return DoFnOutputReceivers.rowReceiver(
          (OutputInterface<T>) output, tag, (SchemaCoder<T>) outputCoder);
    }
  }

  /** Returns a {@link OutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static <T> OutputReceiver<T> windowedReceiver(
      OutputInterface<T> output, @Nullable TupleTag<T> outputTag) {
    return new WindowedOutputReceiver<>(output, outputTag);
  }

  /** Returns a {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static MultiOutputReceiver windowedMultiReceiver(
      OutputInterface<?> output, @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
    return new WindowedMultiOutputReceiver(output, outputCoders);
  }
  /**
   * Returns a {@link OutputReceiver} that automatically converts a {@link Row} to the user's output
   * type and delegates to {@link WindowedOutputReceiver}.
   */
  public static <T> OutputReceiver<Row> rowReceiver(
      OutputInterface<T> output, @Nullable TupleTag<T> outputTag, SchemaCoder<T> schemaCoder) {
    return new RowOutputReceiver<>(output, outputTag, schemaCoder);
  }
}
