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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Streaming translator for {@link Impulse}.
 *
 * <p>Emits a single empty byte array in a streaming micro-batch, advancing the watermark to
 * infinity upon completion so that pipelines mixing Impulse-generated elements (e.g. {@code
 * Create.of} in {@code PAssert}) with unbounded streams can safely union without schema or
 * batch/streaming mismatch.
 */
public class ImpulseStreamingTranslator
    extends TransformTranslator<PBegin, PCollection<byte[]>, Impulse> {

  public ImpulseStreamingTranslator() {
    super(0.05f);
  }

  @Override
  protected void translate(Impulse transform, Context cxt) {
    PCollection<byte[]> output = cxt.getOutput();
    Coder<byte[]> elementCoder = output.getCoder();

    WindowedValues.FullWindowedValueCoder<byte[]> payloadCoder =
        WindowedValues.getFullCoder(elementCoder, GlobalWindow.Coder.INSTANCE);

    SparkStructuredStreamingPipelineOptions options =
        cxt.getOptions().as(SparkStructuredStreamingPipelineOptions.class);

    Dataset<Row> rows =
        UnboundedSourceDataset.of(
            cxt.getSparkSession(),
            new ImpulseSource(),
            payloadCoder,
            options,
            cxt.getCurrentTransform().getFullName());

    Encoder<WindowedValue<byte[]>> encoder =
        cxt.windowedEncoder(elementCoder, GlobalWindow.Coder.INSTANCE);

    Dataset<WindowedValue<byte[]>> dataset =
        rows.select(col(UnboundedSourceDataset.COL_PAYLOAD))
            .as(Encoders.BINARY())
            .filter(new FilterNonEmptyPayload())
            .map(new DecodePayload<>(payloadCoder), encoder);

    cxt.putDataset(output, dataset);
  }

  private static final class FilterNonEmptyPayload implements FilterFunction<byte[]> {
    @Override
    public boolean call(byte[] payload) {
      return payload != null && payload.length > 0;
    }
  }

  private static final class DecodePayload<T> implements MapFunction<byte[], WindowedValue<T>> {
    private final Coder<WindowedValue<T>> coder;

    DecodePayload(Coder<WindowedValue<T>> coder) {
      this.coder = coder;
    }

    @Override
    public WindowedValue<T> call(byte[] payload) {
      return CoderHelpers.fromByteArray(payload, coder);
    }
  }

  /** An unbounded source that produces exactly one empty byte array and then finishes. */
  private static final class ImpulseSource
      extends UnboundedSource<byte[], ImpulseSource.ImpulseCheckpointMark> {

    @Override
    public List<? extends UnboundedSource<byte[], ImpulseCheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<byte[]> createReader(
        PipelineOptions options, @Nullable ImpulseCheckpointMark checkpointMark) {
      return new ImpulseReader(this, checkpointMark != null && checkpointMark.done);
    }

    @Override
    public Coder<ImpulseCheckpointMark> getCheckpointMarkCoder() {
      return ImpulseCheckpointMarkCoder.of();
    }

    @Override
    public Coder<byte[]> getOutputCoder() {
      return ByteArrayCoder.of();
    }

    static final class ImpulseCheckpointMark
        implements UnboundedSource.CheckpointMark, Serializable {
      final boolean done;

      ImpulseCheckpointMark(boolean done) {
        this.done = done;
      }

      @Override
      public void finalizeCheckpoint() {}
    }

    static final class ImpulseCheckpointMarkCoder extends AtomicCoder<ImpulseCheckpointMark> {
      private static final ImpulseCheckpointMarkCoder INSTANCE = new ImpulseCheckpointMarkCoder();

      public static ImpulseCheckpointMarkCoder of() {
        return INSTANCE;
      }

      @Override
      public void encode(ImpulseCheckpointMark value, OutputStream outStream) throws IOException {
        outStream.write(value.done ? 1 : 0);
      }

      @Override
      public ImpulseCheckpointMark decode(InputStream inStream) throws IOException {
        return new ImpulseCheckpointMark(inStream.read() != 0);
      }
    }

    private static final class ImpulseReader extends UnboundedReader<byte[]> {
      private final ImpulseSource source;
      private boolean done;
      private boolean started;

      ImpulseReader(ImpulseSource source, boolean done) {
        this.source = source;
        this.done = done;
      }

      @Override
      public boolean start() {
        if (done) {
          return false;
        }
        started = true;
        done = true;
        return true;
      }

      @Override
      public boolean advance() {
        return false;
      }

      @Override
      public byte[] getCurrent() {
        if (!started) {
          throw new NoSuchElementException();
        }
        return EMPTY_BYTE_ARRAY;
      }

      @Override
      public Instant getCurrentTimestamp() {
        if (!started) {
          throw new NoSuchElementException();
        }
        return BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      @Override
      public void close() {}

      @Override
      public Instant getWatermark() {
        return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new ImpulseCheckpointMark(done);
      }

      @Override
      public UnboundedSource<byte[], ?> getCurrentSource() {
        return source;
      }
    }
  }
}
