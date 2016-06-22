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
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;

import com.google.common.base.Function;
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
 * A {@code BoundedSource} for reading files resident in a Hadoop filesystem (HDFS) using a
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
 * PCollection<KV<MyKey, MyValue>> records = Read.from(mySource);
 * }
 * </pre>
 *
 * <p>The {@link HDFSFileSource#readFrom} method is a convenience method
 * that returns a read transform. For example:
 *
 * <pre>
 * {@code
 * PCollection<KV<MyKey, MyValue>> records = HDFSFileSource.readFrom(path,
 *   MyInputFormat.class, MyKey.class, MyValue.class);
 * }
 * </pre>
 *
 * Implementation note: Since Hadoop's {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
 * determines the input splits, this class extends {@link BoundedSource} rather than
 * {@link org.apache.beam.sdk.io.OffsetBasedSource}, since the latter
 * dictates input splits.

 * @param <K> The type of keys to be read from the source.
 * @param <V> The type of values to be read from the source.
 */
public class HDFSFileSource<K, V> extends BoundedSource<KV<K, V>> {
  private static final long serialVersionUID = 0L;

  protected final String filepattern;
  protected final Class<? extends FileInputFormat<?, ?>> formatClass;
  protected final Class<K> keyClass;
  protected final Class<V> valueClass;
  protected final SerializableSplit serializableSplit;

  /**
   * Creates a {@code Read} transform that will read from an {@code HDFSFileSource}
   * with the given file name or pattern ("glob") using the given Hadoop
   * {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class.
   */
  public static <K, V, T extends FileInputFormat<K, V>> Read.Bounded<KV<K, V>> readFrom(
      String filepattern, Class<T> formatClass, Class<K> keyClass, Class<V> valueClass) {
    return Read.from(from(filepattern, formatClass, keyClass, valueClass));
  }

  /**
   * Creates a {@code HDFSFileSource} that reads from the given file name or pattern ("glob")
   * using the given Hadoop {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat},
   * with key-value types specified by the given key class and value class.
   */
  public static <K, V, T extends FileInputFormat<K, V>> HDFSFileSource<K, V> from(
      String filepattern, Class<T> formatClass, Class<K> keyClass, Class<V> valueClass) {
    @SuppressWarnings("unchecked")
    HDFSFileSource<K, V> source = (HDFSFileSource<K, V>)
        new HDFSFileSource(filepattern, formatClass, keyClass, valueClass);
    return source;
  }

  /**
   * Create a {@code HDFSFileSource} based on a file or a file pattern specification.
   */
  protected HDFSFileSource(String filepattern,
                           Class<? extends FileInputFormat<?, ?>> formatClass, Class<K> keyClass,
                           Class<V> valueClass) {
    this(filepattern, formatClass, keyClass, valueClass, null);
  }

  /**
   * Create a {@code HDFSFileSource} based on a single Hadoop input split, which won't be
   * split up further.
   */
  protected HDFSFileSource(String filepattern,
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
    checkNotNull(filepattern, "need to set the filepattern of a HDFSFileSource");
    checkNotNull(formatClass, "need to set the format class of a HDFSFileSource");
    checkNotNull(keyClass, "need to set the key class of a HDFSFileSource");
    checkNotNull(valueClass, "need to set the value class of a HDFSFileSource");
  }

  @Override
  public List<? extends BoundedSource<KV<K, V>>> splitIntoBundles(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    if (serializableSplit == null) {
      return Lists.transform(computeSplits(desiredBundleSizeBytes),
          new Function<InputSplit, BoundedSource<KV<K, V>>>() {
        @Override
        public BoundedSource<KV<K, V>> apply(@Nullable InputSplit inputSplit) {
          return new HDFSFileSource<>(filepattern, formatClass, keyClass,
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

  protected List<InputSplit> computeSplits(long desiredBundleSizeBytes) throws IOException,
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
      return new HDFSFileReader<>(this, filepattern, formatClass);
    } else {
      return new HDFSFileReader<>(this, filepattern, formatClass,
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

  static class HDFSFileReader<K, V> extends BoundedSource.BoundedReader<KV<K, V>> {

    private final BoundedSource<KV<K, V>> source;
    private final String filepattern;
    private final Class formatClass;
    protected Job job;

    private FileInputFormat<?, ?> format;
    private TaskAttemptContext attemptContext;
    private List<InputSplit> splits;
    private ListIterator<InputSplit> splitsIterator;
    private Configuration conf;
    protected RecordReader<K, V> currentReader;
    private KV<K, V> currentPair;
    private volatile boolean done = false;

    /**
     * Create a {@code HDFSFileReader} based on a file or a file pattern specification.
     */
    public HDFSFileReader(BoundedSource<KV<K, V>> source, String filepattern,
                          Class<? extends FileInputFormat<?, ?>> formatClass) throws IOException {
      this(source, filepattern, formatClass, null);
    }

    /**
     * Create a {@code HDFSFileReader} based on a single Hadoop input split.
     */
    public HDFSFileReader(BoundedSource<KV<K, V>> source, String filepattern,
                          Class<? extends FileInputFormat<?, ?>> formatClass, InputSplit split)
            throws IOException {
      this.source = source;
      this.filepattern = filepattern;
      this.formatClass = formatClass;
      if (split != null) {
        this.splits = ImmutableList.of(split);
        this.splitsIterator = splits.listIterator();
      }
      this.job = Job.getInstance(); // new instance
    }

    @Override
    public boolean start() throws IOException {
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
          done = true;
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @SuppressWarnings("unchecked")
    protected KV<K, V> nextPair() throws IOException, InterruptedException {
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
    public final long getSplitPointsRemaining() {
      if (done) {
        return 0;
      }
      // This source does not currently support dynamic work rebalancing, so remaining
      // parallelism is always 1.
      return 1;
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
