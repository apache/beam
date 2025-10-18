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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.util.OutputBuilderSupplier;
import org.apache.beam.sdk.util.WindowedValueReceiver;
import org.apache.beam.sdk.values.OutputBuilder;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Common {@link OutputReceiver} and {@link MultiOutputReceiver} classes. */
@Internal
public class DoFnOutputReceivers {

  private static class RowOutputReceiver<T> implements OutputReceiver<Row> {
    private final @Nullable TupleTag<T> tag;
    private final DoFn<?, ?>.WindowedContext context;
    private final OutputBuilderSupplier builderSupplier;
    SchemaCoder<T> schemaCoder;

    private RowOutputReceiver(
        DoFn<?, ?>.WindowedContext context,
        OutputBuilderSupplier builderSupplier,
        @Nullable TupleTag<T> outputTag,
        SchemaCoder<T> schemaCoder) {
      this.context = context;
      this.builderSupplier = builderSupplier;
      this.tag = outputTag;
      this.schemaCoder = schemaCoder;
    }

    @Override
    public OutputBuilder<Row> builder(Row value) {
      // assigning to final variable allows static analysis to know it
      // will not change between now and when receiver is invoked
      final TupleTag<T> tag = this.tag;
      if (tag == null) {
        return builderSupplier
            .builder(value)
            .setValue(value)
            .setReceiver(
                rowWithMetadata -> {
                  ((DoFn<?, T>.WindowedContext) context)
                      .outputWindowedValue(
                          schemaCoder.getFromRowFunction().apply(rowWithMetadata.getValue()),
                          rowWithMetadata.getTimestamp(),
                          rowWithMetadata.getWindows(),
                          rowWithMetadata.getPaneInfo());
                });

      } else {
        checkStateNotNull(tag);
        return builderSupplier
            .builder(value)
            .setReceiver(
                rowWithMetadata -> {
                  context.outputWindowedValue(
                      tag,
                      schemaCoder.getFromRowFunction().apply(rowWithMetadata.getValue()),
                      rowWithMetadata.getTimestamp(),
                      rowWithMetadata.getWindows(),
                      rowWithMetadata.getPaneInfo());
                });
      }
    }
  }

  /**
   * OutputReceiver that delegates all its core functionality to DoFn.WindowedContext which predates
   * OutputReceiver and has most of the same methods.
   */
  private static class WindowedContextOutputReceiver<T>
      implements OutputReceiver<T>, WindowedValueReceiver<T> {
    private final OutputBuilderSupplier builderSupplier;
    DoFn<?, ?>.WindowedContext context;
    @Nullable TupleTag<T> outputTag;

    public WindowedContextOutputReceiver(
        DoFn<?, ?>.WindowedContext context,
        OutputBuilderSupplier builderSupplier,
        @Nullable TupleTag<T> outputTag) {
      this.context = context;
      this.builderSupplier = builderSupplier;
      this.outputTag = outputTag;
    }

    @Override
    public OutputBuilder<T> builder(T value) {
      return WindowedValues.builder(builderSupplier.builder(value)).setReceiver(this);
    }

    @Override
    public void output(WindowedValue<T> windowedValue) {
      if (outputTag != null) {
        context.outputWindowedValue(
            outputTag,
            windowedValue.getValue(),
            windowedValue.getTimestamp(),
            windowedValue.getWindows(),
            windowedValue.getPaneInfo());
      } else {
        ((DoFn<?, T>.WindowedContext) context)
            .outputWindowedValue(
                windowedValue.getValue(),
                windowedValue.getTimestamp(),
                windowedValue.getWindows(),
                windowedValue.getPaneInfo());
      }
    }
  }

  private static class WindowedContextMultiOutputReceiver implements MultiOutputReceiver {
    private final OutputBuilderSupplier builderSupplier;
    private final DoFn<?, ?>.WindowedContext context;
    @Nullable Map<TupleTag<?>, Coder<?>> outputCoders;

    public WindowedContextMultiOutputReceiver(
        DoFn<?, ?>.WindowedContext context,
        OutputBuilderSupplier builderSupplier,
        @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
      this.context = context;
      this.builderSupplier = builderSupplier;
      this.outputCoders = outputCoders;
    }

    // This exists for backwards compatibility with the Dataflow runner, and will be removed.
    public WindowedContextMultiOutputReceiver(
        DoFn<?, ?>.WindowedContext context, OutputBuilderSupplier builderSupplier) {
      this.context = context;
      this.builderSupplier = builderSupplier;
    }

    @Override
    public <T> OutputReceiver<T> get(TupleTag<T> tag) {
      return DoFnOutputReceivers.windowedReceiver(context, builderSupplier, tag);
    }

    @Override
    public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
      Coder<T> outputCoder = (Coder<T>) checkNotNull(outputCoders).get(tag);
      checkStateNotNull(outputCoder, "No output tag for %s ", tag);
      checkState(
          outputCoder instanceof SchemaCoder,
          "Output with tag " + tag + " must have a schema in order to call getRowReceiver");
      return DoFnOutputReceivers.rowReceiver(
          context, builderSupplier, tag, (SchemaCoder<T>) outputCoder);
    }
  }

  /** Returns a {@link OutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static <T> OutputReceiver<T> windowedReceiver(
      DoFn<?, ?>.WindowedContext context,
      OutputBuilderSupplier builderSupplier,
      @Nullable TupleTag<T> outputTag) {
    return new WindowedContextOutputReceiver<>(context, builderSupplier, outputTag);
  }

  /** Returns a {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}. */
  public static MultiOutputReceiver windowedMultiReceiver(
      DoFn<?, ?>.WindowedContext context,
      OutputBuilderSupplier builderSupplier,
      @Nullable Map<TupleTag<?>, Coder<?>> outputCoders) {
    return new WindowedContextMultiOutputReceiver(context, builderSupplier, outputCoders);
  }

  /**
   * Returns a {@link MultiOutputReceiver} that delegates to a {@link DoFn.WindowedContext}.
   *
   * <p>This exists for backwards-compatibility with the Dataflow runner, and will be removed.
   */
  public static MultiOutputReceiver windowedMultiReceiver(
      DoFn<?, ?>.WindowedContext context, OutputBuilderSupplier builderSupplier) {
    return new WindowedContextMultiOutputReceiver(context, builderSupplier);
  }

  /**
   * Returns a {@link OutputReceiver} that automatically converts a {@link Row} to the user's output
   * type and delegates to {@link WindowedContextOutputReceiver}.
   */
  public static <T> OutputReceiver<Row> rowReceiver(
      DoFn<?, ?>.WindowedContext context,
      OutputBuilderSupplier builderSupplier,
      @Nullable TupleTag<T> outputTag,
      SchemaCoder<T> schemaCoder) {
    return new RowOutputReceiver<>(context, builderSupplier, outputTag, schemaCoder);
  }
}
