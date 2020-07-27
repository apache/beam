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
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Common {@link OutputReceiver} and {@link MultiOutputReceiver} classes. */
@Internal
public class DoFnOutputReceivers {
  private static class RowOutputReceiver<T> implements OutputReceiver<Row> {
    WindowedContextOutputReceiver<T> outputReceiver;
    SchemaCoder<T> schemaCoder;

    public RowOutputReceiver(
        DoFn<?, ?>.WindowedContext context,
        @Nullable TupleTag<T> outputTag,
        SchemaCoder<T> schemaCoder) {
      outputReceiver = new WindowedContextOutputReceiver<>(context, outputTag);
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

  private static class WindowedContextOutputReceiver<T> implements OutputReceiver<T> {
    DoFn<?, ?>.WindowedContext context;
    @Nullable TupleTag<T> outputTag;

    public WindowedContextOutputReceiver(
        DoFn<?, ?>.WindowedContext context, @Nullable TupleTag<T> outputTag) {
      this.context = context;
      this.outputTag = outputTag;
    }

    @Override
    public void output(T output) {
      if (outputTag != null) {
        context.output(outputTag, output);
      } else {
        ((DoFn<?, T>.WindowedContext) context).output(output);
      }
    }

    @Override
    public void outputWithTimestamp(T output, Instant timestamp) {
      if (outputTag != null) {
        context.outputWithTimestamp(outputTag, output, timestamp);
      } else {
        ((DoFn<?, T>.WindowedContext) context).outputWithTimestamp(output, timestamp);
      }
    }
  }

  private static class WindowedContextMultiOutputReceiver implements MultiOutputReceiver {
    DoFn<?, ?>.WindowedContext context;
    @Nullable Map<TupleTag<?>, Coder<?>> outputCoders;

    public WindowedContextMultiOutputReceiver(
        DoFn<?, ?>.WindowedContext context, @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
      this.context = context;
      this.outputCoders = outputCoders;
    }

    // This exists for backwards compatibility with the Dataflow runner, and will be removed.
    public WindowedContextMultiOutputReceiver(DoFn<?, ?>.WindowedContext context) {
      this.context = context;
    }

    @Override
    public <T> OutputReceiver<T> get(TupleTag<T> tag) {
      return DoFnOutputReceivers.windowedReceiver(context, tag);
    }

    @Override
    public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
      Coder<T> outputCoder = (Coder<T>) checkNotNull(outputCoders).get(tag);
      checkState(outputCoder != null, "No output tag for " + tag);
      checkState(
          outputCoder instanceof SchemaCoder,
          "Output with tag " + tag + " must have a schema in order to call " + " getRowReceiver");
      return DoFnOutputReceivers.rowReceiver(context, tag, (SchemaCoder<T>) outputCoder);
    }
  }

  /** Returns a {@link OutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static <T> OutputReceiver<T> windowedReceiver(
      DoFn<?, ?>.WindowedContext context, @Nullable TupleTag<T> outputTag) {
    return new WindowedContextOutputReceiver<>(context, outputTag);
  }

  /** Returns a {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static <T> MultiOutputReceiver windowedMultiReceiver(
      DoFn<?, ?>.WindowedContext context, @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
    return new WindowedContextMultiOutputReceiver(context, outputCoders);
  }

  /**
   * Returns a {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}.
   *
   * <p>This exists for backwards-compatibility with the Dataflow runner, and will be removed.
   */
  public static <T> MultiOutputReceiver windowedMultiReceiver(DoFn<?, ?>.WindowedContext context) {
    return new WindowedContextMultiOutputReceiver(context);
  }

  /**
   * Returns a {@link OutputReceiver} that automatically converts a {@link Row} to the user's output
   * type and delegates to {@link WindowedContextOutputReceiver}.
   */
  public static <T> OutputReceiver<Row> rowReceiver(
      DoFn<?, ?>.WindowedContext context,
      @Nullable TupleTag<T> outputTag,
      SchemaCoder<T> schemaCoder) {
    return new RowOutputReceiver<>(context, outputTag, schemaCoder);
  }
}
