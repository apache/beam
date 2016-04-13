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
package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * A {@code BoundedSource} for reading files resident in a Hadoop filesystem using a
 * Hadoop file-based input format.
 *
 * <p>To read a {@link com.google.cloud.dataflow.sdk.values.PCollection} of
 * {@link com.google.cloud.dataflow.sdk.values.KV} key-value pairs from one or more
 * Hadoop files, use {@link HadoopFileSource#from} to specify the path(s) of the files to
 * read, the Hadoop {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}, the
 * key class and the value class.
 *
 * <p>A {@code HadoopFileSource} can be read from using the
 * {@link com.google.cloud.dataflow.sdk.io.Read} transform. For example:
 *
 * <pre>
 * {@code
 * HadoopFileSource<K, V> source = HadoopFileSource.from(path, MyInputFormat.class,
 *   MyKey.class, MyValue.class);
 * PCollection<KV<MyKey, MyValue>> records = Read.from(mySource);
 * }
 * </pre>
 *
 * <p>The {@link HadoopFileSource#readFrom} method is a convenience method
 * that returns a read transform. For example:
 *
 * <pre>
 * {@code
 * PCollection<KV<MyKey, MyValue>> records = HadoopFileSource.readFrom(path,
 *   MyInputFormat.class, MyKey.class, MyValue.class);
 * }
 * </pre>
 *
 * Implementation note: Since Hadoop's {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
 * determines the input splits, this class extends {@link BoundedSource} rather than
 * {@link com.google.cloud.dataflow.sdk.io.OffsetBasedSource}, since the latter
 * dictates input splits.

 * @param <K> The type of keys to be read from the source.
 * @param <V> The type of values to be read from the source.
 */
public class HadoopFileSource<K, V> extends BoundedSource<KV<K, V>> {
  private static final long serialVersionUID = 0L;

  private final String filepattern;
  private final Class<? extends FileInputFormat<?, ?>> formatClass;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final SerializableSplit serializableSplit;

  /**
   * Creates a {@code Read} transform that will read from an {@code HadoopFileSource}
   * with the given file name or pattern ("glob") using the given Hadoop
   * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class.
   */
  public static <K, V, T extends FileInputFormat<K, V>> Read.Bounded<KV<K, V>> readFrom(
      String filepattern, Class<T> formatClass, Class<K> keyClass, Class<V> valueClass) {
    return Read.from(from(filepattern, formatClass, keyClass, valueClass));
  }

  /**
   * Creates a {@code HadoopFileSource} that reads from the given file name or pattern ("glob")
   * using the given Hadoop {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class.
   */
  public static <K, V, T extends FileInputFormat<K, V>> HadoopFileSource<K, V> from(
      String filepattern, Class<T> formatClass, Class<K> keyClass, Class<V> valueClass) {
    @SuppressWarnings("unchecked")
    HadoopFileSource<K, V> source = (HadoopFileSource<K, V>)
        new HadoopFileSource(filepattern, formatClass, keyClass, valueClass);
    return source;
  }

  /**
   * Create a {@code HadoopFileSource} based on a file or a file pattern specification.
   */
  private HadoopFileSource(String filepattern,
      Class<? extends FileInputFormat<?, ?>> formatClass, Class<K> keyClass,
      Class<V> valueClass) {
    this(filepattern, formatClass, keyClass, valueClass, null);
  }

  /**
   * Create a {@code HadoopFileSource} based on a single Hadoop input split, which won't be
   * split up further.
   */
  private HadoopFileSource(String filepattern,
      Class<? extends FileInputFormat<?, ?>> formatClass, Class<K> keyClass,
      Class<V> valueClass, SerializableSplit serializableSplit) {
    this.filepattern = filepattern;
    this.formatClass = formatClass;
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.serializableSplit = serializableSplit;
  }

  public String getFilepattern() {
    return filepattern;
  }

  public Class<? extends FileInputFormat<?, ?>> getFormatClass() {
    return formatClass;
  }

  public Class<K> getKeyClass() {
    return keyClass;
  }

  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(filepattern,
        "need to set the filepattern of a HadoopFileSource");
    Preconditions.checkNotNull(formatClass,
        "need to set the format class of a HadoopFileSource");
    Preconditions.checkNotNull(keyClass,
        "need to set the key class of a HadoopFileSource");
    Preconditions.checkNotNull(valueClass,
        "need to set the value class of a HadoopFileSource");
  }

  @Override
  public List<? extends BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<KV<K, V>>>() {
        @Nullable @Override
        public BoundedSource<KV<K, V>> apply(@Nullable InputSplit inputSplit) {
          return new HadoopFileSource<K, V>(filepattern, formatClass, keyClass,
              valueClass, new SerializableSplit(inputSplit));
        }
      });
    } else {
      return ImmutableList.of(this);
    }
  }

  private FileInputFormat<?, ?> createFormat(Job job) throws IOException, IllegalAccessException,
      InstantiationException {
    Path path = new Path(filepattern);
    FileInputFormat.addInputPath(job, path);
    return formatClass.newInstance();
  }

  private List<InputSplit> computeSplits(long desiredBundleSizeBytes) throws IOException,
      IllegalAccessException, InstantiationException {
    Job job = Job.getInstance();
    FileInputFormat.setMinInputSplitSize(job, desiredBundleSizeBytes);
    FileInputFormat.setMaxInputSplitSize(job, desiredBundleSizeBytes);
    return createFormat(job).getSplits(job);
  }

  @Override
  public BoundedReader<KV<K, V>> createReader(PipelineOptions options) throws IOException {
    this.validate();

    if (serializableSplit == null) {
      return new HadoopFileReader<>(this, filepattern, formatClass);
    } else {
      return new HadoopFileReader<>(this, filepattern, formatClass,
          serializableSplit.getSplit());
    }
  }

  @Override
  public Coder<KV<K, V>> getDefaultOutputCoder() {
    return KvCoder.of(getDefaultCoder(keyClass), getDefaultCoder(valueClass));
  }

  @SuppressWarnings("unchecked")
  private <T> Coder<T> getDefaultCoder(Class<T> c) {
    if (Writable.class.isAssignableFrom(c)) {
      Class<? extends Writable> writableClass = (Class<? extends Writable>) c;
      return (Coder<T>) WritableCoder.of(writableClass);
    } else if (Void.class.equals(c)) {
      return (Coder<T>) VoidCoder.of();
    }
    // TODO: how to use registered coders here?
    throw new IllegalStateException("Cannot find coder for " + c);
  }

  // BoundedSource

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    long size = 0;
    try {
      Job job = Job.getInstance(); // new instance
      for (FileStatus st : listStatus(createFormat(job), job)) {
        size += st.getLen();
      }
    } catch (IOException | NoSuchMethodException | InvocationTargetException
        | IllegalAccessException | InstantiationException e) {
      // ignore, and return 0
    }
    return size;
  }

  private <K, V> List<FileStatus> listStatus(FileInputFormat<K, V> format,
      JobContext jobContext) throws NoSuchMethodException, InvocationTargetException,
      IllegalAccessException {
    // FileInputFormat#listStatus is protected, so call using reflection
    Method listStatus = FileInputFormat.class.getDeclaredMethod("listStatus", JobContext.class);
    listStatus.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<FileStatus> stat = (List<FileStatus>) listStatus.invoke(format, jobContext);
    return stat;
  }

  @Override
  public boolean producesSortedKeys(PipelineOptions options) throws Exception {
    return false;
  }

  static class HadoopFileReader<K, V> extends BoundedSource.BoundedReader<KV<K, V>> {

    private final BoundedSource<KV<K, V>> source;
    private final String filepattern;
    private final Class formatClass;

    private FileInputFormat<?, ?> format;
    private TaskAttemptContext attemptContext;
    private List<InputSplit> splits;
    private ListIterator<InputSplit> splitsIterator;
    private Configuration conf;
    private RecordReader<K, V> currentReader;
    private KV<K, V> currentPair;

    /**
     * Create a {@code HadoopFileReader} based on a file or a file pattern specification.
     */
    public HadoopFileReader(BoundedSource<KV<K, V>> source, String filepattern,
        Class<? extends FileInputFormat<?, ?>> formatClass) {
      this(source, filepattern, formatClass, null);
    }

    /**
     * Create a {@code HadoopFileReader} based on a single Hadoop input split.
     */
    public HadoopFileReader(BoundedSource<KV<K, V>> source, String filepattern,
        Class<? extends FileInputFormat<?, ?>> formatClass, InputSplit split) {
      this.source = source;
      this.filepattern = filepattern;
      this.formatClass = formatClass;
      if (split != null) {
        this.splits = ImmutableList.of(split);
        this.splitsIterator = splits.listIterator();
      }
    }

    @Override
    public boolean start() throws IOException {
      Job job = Job.getInstance(); // new instance
      Path path = new Path(filepattern);
      FileInputFormat.addInputPath(job, path);

      try {
        @SuppressWarnings("unchecked")
        FileInputFormat<K, V> f = (FileInputFormat<K, V>) formatClass.newInstance();
        this.format = f;
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException("Cannot instantiate file input format " + formatClass, e);
      }
      this.attemptContext = new TaskAttemptContextImpl(job.getConfiguration(),
          new TaskAttemptID());

      if (splitsIterator == null) {
        this.splits = format.getSplits(job);
        this.splitsIterator = splits.listIterator();
      }
      this.conf = job.getConfiguration();
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
            InputSplit nextSplit = splitsIterator.next();
            @SuppressWarnings("unchecked")
            RecordReader<K, V> reader =
                (RecordReader<K, V>) format.createRecordReader(nextSplit, attemptContext);
            if (currentReader != null) {
              currentReader.close();
            }
            currentReader = reader;
            currentReader.initialize(nextSplit, attemptContext);
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

    @Override
    public KV<K, V> getCurrent() throws NoSuchElementException {
      if (currentPair == null) {
        throw new NoSuchElementException();
      }
      return currentPair;
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
    public BoundedSource<KV<K, V>> getCurrentSource() {
      return source;
    }

    // BoundedReader

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

    @Override
    public BoundedSource<KV<K, V>> splitAtFraction(double fraction) {
      // Not yet supported. To implement this, the sizes of the splits should be used to
      // calculate the remaining splits that constitute the given fraction, then a
      // new source backed by those splits should be returned.
      return null;
    }
  }

  /**
   * A wrapper to allow Hadoop {@link org.apache.hadoop.mapreduce.InputSplit}s to be
   * serialized using Java's standard serialization mechanisms. Note that the InputSplit
   * has to be Writable (which most are).
   */
  public static class SerializableSplit implements Externalizable {
    private static final long serialVersionUID = 0L;

    private InputSplit split;

    public SerializableSplit() {
    }

    public SerializableSplit(InputSplit split) {
      Preconditions.checkArgument(split instanceof Writable, "Split is not writable: "
          + split);
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
