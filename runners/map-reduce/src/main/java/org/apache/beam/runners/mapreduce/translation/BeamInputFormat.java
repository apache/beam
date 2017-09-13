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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Adaptor from Beam {@link BoundedSource} to MapReduce {@link InputFormat}.
 */
public class BeamInputFormat<T> extends InputFormat {

  public static final String BEAM_SERIALIZED_BOUNDED_SOURCE = "beam-serialized-bounded-source";
  public static final String BEAM_SERIALIZED_PIPELINE_OPTIONS = "beam-serialized-pipeline-options";

  private static final long DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES = 5 * 1000 * 1000;

  private List<ReadOperation.TaggedSource> sources;
  private SerializablePipelineOptions options;

  public BeamInputFormat() {
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    String serializedBoundedSource = context.getConfiguration().get(BEAM_SERIALIZED_BOUNDED_SOURCE);
    String serializedPipelineOptions =
        context.getConfiguration().get(BEAM_SERIALIZED_PIPELINE_OPTIONS);
    if (Strings.isNullOrEmpty(serializedBoundedSource)
        || Strings.isNullOrEmpty(serializedPipelineOptions)) {
      return ImmutableList.of();
    }
    sources = (List<ReadOperation.TaggedSource>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedBoundedSource), "TaggedSources");
    options = ((SerializablePipelineOptions) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedPipelineOptions), "SerializablePipelineOptions"));

    try {

      return FluentIterable.from(sources)
          .transformAndConcat(
              new Function<ReadOperation.TaggedSource, Iterable<ReadOperation.TaggedSource>>() {
                @Override
                public Iterable<ReadOperation.TaggedSource> apply(
                    final ReadOperation.TaggedSource taggedSource) {
                  try {
                    return FluentIterable.from(taggedSource.getSource().split(
                        DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES, options.get()))
                        .transform(new Function<BoundedSource<?>, ReadOperation.TaggedSource>() {
                          @Override
                          public ReadOperation.TaggedSource apply(BoundedSource<?> input) {
                            return ReadOperation.TaggedSource.of(
                                taggedSource.getStepName(), input, taggedSource.getTag());
                          }});
                  } catch (Exception e) {
                    Throwables.throwIfUnchecked(e);
                    throw new RuntimeException(e);
                  }
                }
              })
          .transform(new Function<ReadOperation.TaggedSource, InputSplit>() {
            @Override
            public InputSplit apply(ReadOperation.TaggedSource taggedSource) {
              return new BeamInputSplit(taggedSource.getStepName(), taggedSource.getSource(),
                  options, taggedSource.getTag());
            }})
          .toList();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return ((BeamInputSplit) split).createReader();
  }

  private static class BeamInputSplit<T> extends InputSplit implements Writable {
    private String stepName;
    private BoundedSource<T> boundedSource;
    private SerializablePipelineOptions options;
    private TupleTag<?> tupleTag;

    public BeamInputSplit() {
    }

    public BeamInputSplit(
        String stepName,
        BoundedSource<T> boundedSource,
        SerializablePipelineOptions options,
        TupleTag<?> tupleTag) {
      this.stepName = checkNotNull(stepName, "stepName");
      this.boundedSource = checkNotNull(boundedSource, "boundedSources");
      this.options = checkNotNull(options, "options");
      this.tupleTag = checkNotNull(tupleTag, "tupleTag");
    }

    public BeamRecordReader<T> createReader() throws IOException {
      return new BeamRecordReader<>(
          stepName, boundedSource.createReader(options.get()), tupleTag);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      try {
        return boundedSource.getEstimatedSizeBytes(options.get());
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        Throwables.throwIfInstanceOf(e, IOException.class);
        Throwables.throwIfInstanceOf(e, InterruptedException.class);
        throw new RuntimeException(e);
      }
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      StringUtf8Coder.of().encode(stepName, stream);
      SerializableCoder.of(BoundedSource.class).encode(boundedSource, stream);
      SerializableCoder.of(SerializablePipelineOptions.class).encode(options, stream);
      SerializableCoder.of(TupleTag.class).encode(tupleTag, stream);

      byte[] bytes = stream.toByteArray();
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);

      ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
      stepName = StringUtf8Coder.of().decode(inStream);
      boundedSource = SerializableCoder.of(BoundedSource.class).decode(inStream);
      options = SerializableCoder.of(SerializablePipelineOptions.class).decode(inStream);
      tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
    }
  }

  private static class BeamRecordReader<T> extends RecordReader {

    private final String stepName;
    private final BoundedSource.BoundedReader<T> reader;
    private final TupleTag<?> tupleTag;
    private MetricsReporter metricsReporter;
    private boolean started;

    public BeamRecordReader(
        String stepName, BoundedSource.BoundedReader<T> reader, TupleTag<?> tupleTag) {
      this.stepName = checkNotNull(stepName, "stepName");
      this.reader = checkNotNull(reader, "reader");
      this.tupleTag = checkNotNull(tupleTag, "tupleTag");
      this.started = false;
    }

    @Override
    public void initialize(
        InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      this.metricsReporter = new MetricsReporter(context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(
          metricsReporter.getMetricsContainer(stepName))) {
        if (!started) {
          started = true;
          return reader.start();
        } else {
          return reader.advance();
        }
      }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return tupleTag;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      // TODO: this is a hack to handle that reads from materialized PCollections
      // already return WindowedValue.
      if (reader.getCurrent() instanceof WindowedValue) {
        return reader.getCurrent();
      } else {
        return WindowedValue.timestampedValueInGlobalWindow(
            reader.getCurrent(), reader.getCurrentTimestamp());
      }
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      Double progress = reader.getFractionConsumed();
      if (progress != null) {
        return progress.floatValue();
      } else {
        return 0;
      }
    }

    @Override
    public void close() throws IOException {
      reader.close();
      metricsReporter.updateMetrics();
    }
  }
}
