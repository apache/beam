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
package org.apache.beam.sdk.io.hdfs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code BoundedSource} for reading files resident in a Hadoop filesystem using a
 * Hadoop file-based input format.
 *
 * <p>To read a {@link org.apache.beam.sdk.values.PCollection} of
 * {@link org.apache.beam.sdk.values.KV} key-value pairs from one or more
 * Hadoop files, use {@link HDFSFileSource#from} to specify the path(s) of the files to
 * read, the Hadoop {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}, the
 * key class and the value class.
 *
 * <p>A {@code HDFSFileSource} can be read from using the
 * {@link org.apache.beam.sdk.io.Read} transform. For example:
 *
 * <pre>
 * {@code
 * HDFSFileSource<K, V> source = HDFSFileSource.from(path, MyInputFormat.class,
 *   MyKey.class, MyValue.class);
 * PCollection<KV<MyKey, MyValue>> records = pipeline.apply(Read.from(mySource));
 * }
 * </pre>
 *
 * <p>Implementation note: Since Hadoop's
 * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
 * determines the input splits, this class extends {@link BoundedSource} rather than
 * {@link org.apache.beam.sdk.io.OffsetBasedSource}, since the latter
 * dictates input splits.
 * @param <T> the type of elements of the result {@link org.apache.beam.sdk.values.PCollection}.
 * @param <K> the type of keys to be read from the source via {@link FileInputFormat}.
 * @param <V> the type of values to be read from the source via {@link FileInputFormat}.
 */
@AutoValue
@Experimental
public abstract class HDFSFileSource<T, K, V> extends BoundedSource<T> {
  private static final long serialVersionUID = 0L;

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileSource.class);

  public abstract String filepattern();
  public abstract Class<? extends FileInputFormat<K, V>> formatClass();
  public abstract Coder<T> coder();
  public abstract SerializableFunction<KV<K, V>, T> inputConverter();
  public abstract SerializableConfiguration serializableConfiguration();
  public @Nullable abstract SerializableSplit serializableSplit();
  public @Nullable abstract String username();
  public abstract boolean validateSource();

  // =======================================================================
  // Factory methods
  // =======================================================================

  public static <T, K, V, W extends FileInputFormat<K, V>> HDFSFileSource<T, K, V>
  from(String filepattern,
       Class<W> formatClass,
       Coder<T> coder,
       SerializableFunction<KV<K, V>, T> inputConverter) {
    return HDFSFileSource.<T, K, V>builder()
        .setFilepattern(filepattern)
        .setFormatClass(formatClass)
        .setCoder(coder)
        .setInputConverter(inputConverter)
        .setConfiguration(null)
        .setUsername(null)
        .setValidateSource(true)
        .setSerializableSplit(null)
        .build();
  }

  public static <K, V, W extends FileInputFormat<K, V>> HDFSFileSource<KV<K, V>, K, V>
  from(String filepattern,
       Class<W> formatClass,
       Class<K> keyClass,
       Class<V> valueClass) {
    KvCoder<K, V> coder = KvCoder.of(getDefaultCoder(keyClass), getDefaultCoder(valueClass));
    SerializableFunction<KV<K, V>, KV<K, V>> inputConverter =
        new SerializableFunction<KV<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> apply(KV<K, V> input) {
            return input;
          }
        };
    return HDFSFileSource.<KV<K, V>, K, V>builder()
        .setFilepattern(filepattern)
        .setFormatClass(formatClass)
        .setCoder(coder)
        .setInputConverter(inputConverter)
        .setConfiguration(null)
        .setUsername(null)
        .setValidateSource(true)
        .setSerializableSplit(null)
        .build();
  }

  public static HDFSFileSource<String, LongWritable, Text>
  fromText(String filepattern) {
    SerializableFunction<KV<LongWritable, Text>, String> inputConverter =
        new SerializableFunction<KV<LongWritable, Text>, String>() {
      @Override
      public String apply(KV<LongWritable, Text> input) {
        return input.getValue().toString();
      }
    };
    return from(filepattern, TextInputFormat.class, StringUtf8Coder.of(), inputConverter);
  }

  /**
   * Helper to read from Avro source given {@link AvroCoder}. Keep in mind that configuration
   * object is altered to enable Avro input.
   */
  public static <T> HDFSFileSource<T, AvroKey<T>, NullWritable>
  fromAvro(String filepattern, final AvroCoder<T> coder, Configuration conf) {
    Class<AvroKeyInputFormat<T>> formatClass = castClass(AvroKeyInputFormat.class);
    SerializableFunction<KV<AvroKey<T>, NullWritable>, T> inputConverter =
        new SerializableFunction<KV<AvroKey<T>, NullWritable>, T>() {
          @Override
          public T apply(KV<AvroKey<T>, NullWritable> input) {
            try {
              return CoderUtils.clone(coder, input.getKey().datum());
            } catch (CoderException e) {
              throw new RuntimeException(e);
            }
          }
        };
    conf.set("avro.schema.input.key", coder.getSchema().toString());
    return from(filepattern, formatClass, coder, inputConverter).withConfiguration(conf);
  }

  /**
   * Helper to read from Avro source given {@link Schema}. Keep in mind that configuration
   * object is altered to enable Avro input.
   */
  public static HDFSFileSource<GenericRecord, AvroKey<GenericRecord>, NullWritable>
  fromAvro(String filepattern, Schema schema, Configuration conf) {
    return fromAvro(filepattern, AvroCoder.of(schema), conf);
  }

  /**
   * Helper to read from Avro source given {@link Class}. Keep in mind that configuration
   * object is altered to enable Avro input.
   */
  public static <T> HDFSFileSource<T, AvroKey<T>, NullWritable>
  fromAvro(String filepattern, Class<T> cls, Configuration conf) {
    return fromAvro(filepattern, AvroCoder.of(cls), conf);
  }

  // =======================================================================
  // Builder methods
  // =======================================================================

  public abstract HDFSFileSource.Builder<T, K, V> toBuilder();
  public static <T, K, V> HDFSFileSource.Builder builder() {
    return new AutoValue_HDFSFileSource.Builder<>();
  }

  /**
   * AutoValue builder for {@link HDFSFileSource}.
   */
  @AutoValue.Builder
  public abstract static class Builder<T, K, V> {
    public abstract Builder<T, K, V> setFilepattern(String filepattern);
    public abstract Builder<T, K, V> setFormatClass(
        Class<? extends FileInputFormat<K, V>> formatClass);
    public abstract Builder<T, K, V> setCoder(Coder<T> coder);
    public abstract Builder<T, K, V> setInputConverter(
        SerializableFunction<KV<K, V>, T> inputConverter);
    public abstract Builder<T, K, V> setSerializableConfiguration(
        SerializableConfiguration serializableConfiguration);
    public Builder<T, K, V> setConfiguration(Configuration configuration) {
      if (configuration == null) {
        configuration = new Configuration(false);
      }
      return this.setSerializableConfiguration(new SerializableConfiguration(configuration));
    }
    public abstract Builder<T, K, V> setSerializableSplit(SerializableSplit serializableSplit);
    public abstract Builder<T, K, V> setUsername(@Nullable String username);
    public abstract Builder<T, K, V> setValidateSource(boolean validate);
    public abstract HDFSFileSource<T, K, V> build();
  }

  public HDFSFileSource<T, K, V> withConfiguration(@Nullable Configuration configuration) {
    return this.toBuilder().setConfiguration(configuration).build();
  }

  public HDFSFileSource<T, K, V> withUsername(@Nullable String username) {
    return this.toBuilder().setUsername(username).build();
  }

  // =======================================================================
  // BoundedSource
  // =======================================================================

  @Override
  public List<? extends BoundedSource<T>> split(
      final long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    if (serializableSplit() == null) {
      List<InputSplit> inputSplits = UGIHelper.getBestUGI(username()).doAs(
          new PrivilegedExceptionAction<List<InputSplit>>() {
            @Override
            public List<InputSplit> run() throws Exception {
              return computeSplits(desiredBundleSizeBytes, serializableConfiguration());
            }
          });
      return Lists.transform(inputSplits,
          new Function<InputSplit, BoundedSource<T>>() {
            @Override
            public BoundedSource<T> apply(@Nullable InputSplit inputSplit) {
              SerializableSplit serializableSplit = new SerializableSplit(inputSplit);
              return HDFSFileSource.this.toBuilder()
                  .setSerializableSplit(serializableSplit)
                  .build();
            }
          });
    } else {
      return ImmutableList.of(this);
    }
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    long size = 0;

    try {
      // If this source represents a split from split,
      // then return the size of the split, rather then the entire input
      if (serializableSplit() != null) {
        return serializableSplit().getSplit().getLength();
      }

      size += UGIHelper.getBestUGI(username()).doAs(new PrivilegedExceptionAction<Long>() {
        @Override
        public Long run() throws Exception {
          long size = 0;
          Job job = SerializableConfiguration.newJob(serializableConfiguration());
          for (FileStatus st : listStatus(createFormat(job), job)) {
            size += st.getLen();
          }
          return size;
        }
      });
    } catch (IOException e) {
      LOG.warn(
          "Will estimate size of input to be 0 bytes. Can't estimate size of the input due to:", e);
      // ignore, and return 0
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "Will estimate size of input to be 0 bytes. Can't estimate size of the input due to:", e);
      // ignore, and return 0
    }
    return size;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    this.validate();
    return new HDFSFileReader<>(this, filepattern(), formatClass(), serializableSplit());
  }

  @Override
  public void validate() {
    if (validateSource()) {
      try {
        UGIHelper.getBestUGI(username()).doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                final Path pathPattern = new Path(filepattern());
                FileSystem fs = FileSystem.get(pathPattern.toUri(),
                    SerializableConfiguration.newConfiguration(serializableConfiguration()));
                FileStatus[] fileStatuses = fs.globStatus(pathPattern);
                checkState(
                    fileStatuses != null && fileStatuses.length > 0,
                    "Unable to find any files matching %s", filepattern());
                  return null;
                }
              });
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return coder();
  }

  // =======================================================================
  // Helpers
  // =======================================================================

  private List<InputSplit> computeSplits(long desiredBundleSizeBytes,
                                         SerializableConfiguration serializableConfiguration)
      throws IOException, IllegalAccessException, InstantiationException {
    Job job = SerializableConfiguration.newJob(serializableConfiguration);
    FileInputFormat.setMinInputSplitSize(job, desiredBundleSizeBytes);
    FileInputFormat.setMaxInputSplitSize(job, desiredBundleSizeBytes);
    return createFormat(job).getSplits(job);
  }

  private FileInputFormat<K, V> createFormat(Job job)
      throws IOException, IllegalAccessException, InstantiationException {
    Path path = new Path(filepattern());
    FileInputFormat.addInputPath(job, path);
    return formatClass().newInstance();
  }

  private List<FileStatus> listStatus(FileInputFormat<K, V> format, Job job)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // FileInputFormat#listStatus is protected, so call using reflection
    Method listStatus = FileInputFormat.class.getDeclaredMethod("listStatus", JobContext.class);
    listStatus.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<FileStatus> stat = (List<FileStatus>) listStatus.invoke(format, job);
    return stat;
  }

  @SuppressWarnings("unchecked")
  private static <T> Coder<T> getDefaultCoder(Class<T> c) {
    if (Writable.class.isAssignableFrom(c)) {
      Class<? extends Writable> writableClass = (Class<? extends Writable>) c;
      return (Coder<T>) WritableCoder.of(writableClass);
    } else if (Void.class.equals(c)) {
      return (Coder<T>) VoidCoder.of();
    }
    // TODO: how to use registered coders here?
    throw new IllegalStateException("Cannot find coder for " + c);
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> castClass(Class<?> aClass) {
    return (Class<T>) aClass;
  }

  // =======================================================================
  // BoundedReader
  // =======================================================================

  private static class HDFSFileReader<T, K, V> extends BoundedSource.BoundedReader<T> {

    private final HDFSFileSource<T, K, V> source;
    private final String filepattern;
    private final Class<? extends FileInputFormat<K, V>> formatClass;
    private final Job job;

    private List<InputSplit> splits;
    private ListIterator<InputSplit> splitsIterator;

    private Configuration conf;
    private FileInputFormat<?, ?> format;
    private TaskAttemptContext attemptContext;
    private RecordReader<K, V> currentReader;
    private KV<K, V> currentPair;

    HDFSFileReader(
        HDFSFileSource<T, K, V> source,
        String filepattern,
        Class<? extends FileInputFormat<K, V>> formatClass,
        SerializableSplit serializableSplit)
        throws IOException {
      this.source = source;
      this.filepattern = filepattern;
      this.formatClass = formatClass;
      this.job = SerializableConfiguration.newJob(source.serializableConfiguration());

      if (serializableSplit != null) {
        this.splits = ImmutableList.of(serializableSplit.getSplit());
        this.splitsIterator = splits.listIterator();
      }
    }

    @Override
    public boolean start() throws IOException {
      Path path = new Path(filepattern);
      FileInputFormat.addInputPath(job, path);

      conf = job.getConfiguration();
      try {
        format = formatClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException("Cannot instantiate file input format " + formatClass, e);
      }
      attemptContext = new TaskAttemptContextImpl(conf, new TaskAttemptID());

      if (splitsIterator == null) {
        splits = format.getSplits(job);
        splitsIterator = splits.listIterator();
      }

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      try {
        if (currentReader != null && currentReader.nextKeyValue()) {
          currentPair = nextPair();
          return true;
        } else {
          while (splitsIterator.hasNext()) {
            // advance the reader and see if it has records
            final InputSplit nextSplit = splitsIterator.next();
            @SuppressWarnings("unchecked")
            RecordReader<K, V> reader =
                (RecordReader<K, V>) format.createRecordReader(nextSplit, attemptContext);
            if (currentReader != null) {
              currentReader.close();
            }
            currentReader = reader;
            UGIHelper.getBestUGI(source.username()).doAs(new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                currentReader.initialize(nextSplit, attemptContext);
                return null;
              }
            });
            if (currentReader.nextKeyValue()) {
              currentPair = nextPair();
              return true;
            }
            currentReader.close();
            currentReader = null;
          }
          // either no next split or all readers were empty
          currentPair = null;
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (currentPair == null) {
        throw new NoSuchElementException();
      }
      return source.inputConverter().apply(currentPair);
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      currentPair = null;
    }

    @Override
    public BoundedSource<T> getCurrentSource() {
      return source;
    }

    @SuppressWarnings("unchecked")
    private KV<K, V> nextPair() throws IOException, InterruptedException {
      K key = currentReader.getCurrentKey();
      V value = currentReader.getCurrentValue();
      // clone Writable objects since they are reused between calls to RecordReader#nextKeyValue
      if (key instanceof Writable) {
        key = (K) WritableUtils.clone((Writable) key, conf);
      }
      if (value instanceof Writable) {
        value = (V) WritableUtils.clone((Writable) value, conf);
      }
      return KV.of(key, value);
    }

    // =======================================================================
    // Optional overrides
    // =======================================================================

    @Override
    public Double getFractionConsumed() {
      if (currentReader == null) {
        return 0.0;
      }
      if (splits.isEmpty()) {
        return 1.0;
      }
      int index = splitsIterator.previousIndex();
      int numReaders = splits.size();
      if (index == numReaders) {
        return 1.0;
      }
      double before = 1.0 * index / numReaders;
      double after = 1.0 * (index + 1) / numReaders;
      Double fractionOfCurrentReader = getProgress();
      if (fractionOfCurrentReader == null) {
        return before;
      }
      return before + fractionOfCurrentReader * (after - before);
    }

    private Double getProgress() {
      try {
        return (double) currentReader.getProgress();
      } catch (IOException | InterruptedException e) {
        return null;
      }
    }

  }

  // =======================================================================
  // SerializableSplit
  // =======================================================================

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be
   * serialized using Java's standard serialization mechanisms. Note that the InputSplit
   * has to be Writable (which most are).
   */
  protected static class SerializableSplit implements Externalizable {
    private static final long serialVersionUID = 0L;

    private InputSplit split;

    public SerializableSplit() {
    }

    public SerializableSplit(InputSplit split) {
      checkArgument(split instanceof Writable, "Split is not writable: %s", split);
      this.split = split;
    }

    public InputSplit getSplit() {
      return split;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeUTF(split.getClass().getCanonicalName());
      ((Writable) split).write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      String className = in.readUTF();
      try {
        split = (InputSplit) Class.forName(className).newInstance();
        ((Writable) split).readFields(in);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException(e);
      }
    }
  }

}
