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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
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

  private List<BoundedSource<T>> sources;
  private SerializedPipelineOptions options;

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
    sources = (List<BoundedSource<T>>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedBoundedSource), "BoundedSource");
    options = ((SerializedPipelineOptions) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedPipelineOptions), "SerializedPipelineOptions"));

    try {

      return FluentIterable.from(sources)
          .transformAndConcat(new Function<BoundedSource<T>, Iterable<BoundedSource<T>>>() {
            @Override
            public Iterable<BoundedSource<T>> apply(BoundedSource<T> input) {
              try {
                return (Iterable<BoundedSource<T>>) input.split(
                    DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES, options.getPipelineOptions());
              } catch (Exception e) {
                Throwables.throwIfUnchecked(e);
                throw new RuntimeException(e);
              }
            }
          })
          .transform(new Function<BoundedSource<T>, InputSplit>() {
            @Override
            public InputSplit apply(BoundedSource<T> source) {
              return new BeamInputSplit(source, options);
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

  public static class BeamInputSplit<T> extends InputSplit implements Writable {
    private BoundedSource<T> boundedSource;
    private SerializedPipelineOptions options;

    public BeamInputSplit() {
    }

    public BeamInputSplit(BoundedSource<T> boundedSource, SerializedPipelineOptions options) {
      this.boundedSource = checkNotNull(boundedSource, "boundedSources");
      this.options = checkNotNull(options, "options");
    }

    public BeamRecordReader<T> createReader() throws IOException {
      return new BeamRecordReader<>(boundedSource.createReader(options.getPipelineOptions()));
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      try {
        return boundedSource.getEstimatedSizeBytes(options.getPipelineOptions());
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
      SerializableCoder.of(BoundedSource.class).encode(boundedSource, stream);
      SerializableCoder.of(SerializedPipelineOptions.class).encode(options, stream);

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
      boundedSource = SerializableCoder.of(BoundedSource.class).decode(inStream);
      options = SerializableCoder.of(SerializedPipelineOptions.class).decode(inStream);
    }
  }

  private static class BeamRecordReader<T> extends RecordReader {

    private final BoundedSource.BoundedReader<T> reader;
    private boolean started;

    public BeamRecordReader(BoundedSource.BoundedReader<T> reader) {
      this.reader = checkNotNull(reader, "reader");
      this.started = false;
    }

    @Override
    public void initialize(
        InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!started) {
        started = true;
        return reader.start();
      } else {
        return reader.advance();
      }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return "global";
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return WindowedValue.timestampedValueInGlobalWindow(
          reader.getCurrent(), reader.getCurrentTimestamp());
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
    }
  }
}
