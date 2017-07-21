package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
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
public class BeamInputFormat<K, V> extends InputFormat {

  private static final long DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES = 5 * 1000 * 1000;

  private BoundedSource<KV<K, V>> source;
  private PipelineOptions options;

  public BeamInputFormat() {
  }

  public BeamInputFormat(BoundedSource<KV<K, V>> source, PipelineOptions options) {
    this.source = checkNotNull(source, "source");
    this.options = checkNotNull(options, "options");
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    source = (BoundedSource<KV<K,V>>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(context.getConfiguration().get("source")),
        "");
    try {
      return FluentIterable.from(source.split(DEFAULT_DESIRED_BUNDLE_SIZE_SIZE_BYTES, options))
          .transform(new Function<BoundedSource<KV<K, V>>, InputSplit>() {
            @Override
            public InputSplit apply(BoundedSource<KV<K, V>> source) {
              try {
                return new BeamInputSplit(source.getEstimatedSizeBytes(options));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }})
          .toList();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordReader createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    source = (BoundedSource<KV<K,V>>) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(context.getConfiguration().get("source")),
        "");
    return new BeamRecordReader<>(source.createReader(options));
  }

  public static class BeamInputSplit extends InputSplit implements Writable {
    private long estimatedSizeBytes;

    public BeamInputSplit() {
    }

    BeamInputSplit(long estimatedSizeBytes) {
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return estimatedSizeBytes;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(estimatedSizeBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      estimatedSizeBytes = in.readLong();
    }
  }

  private class BeamRecordReader<K, V> extends RecordReader {

    private final BoundedSource.BoundedReader<KV<K, V>> reader;
    private boolean started;

    public BeamRecordReader(BoundedSource.BoundedReader<KV<K, V>> reader) {
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
        return reader.start();
      } else {
        return reader.advance();
      }
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrent().getKey();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return reader.getCurrent().getValue();
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
