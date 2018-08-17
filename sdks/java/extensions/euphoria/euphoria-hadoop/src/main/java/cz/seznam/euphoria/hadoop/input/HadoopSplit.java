/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.io.UnsplittableBoundedSource;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.hadoop.utils.Cloner;
import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

/**
 * Wraps Hadoop {@link InputSplit}
 */
public class HadoopSplit<K, V>
    extends UnsplittableBoundedSource<Pair<K, V>> {

  private final HadoopSource<K, V> source;
  private final Set<String> locations;
  private final Class<? extends InputSplit> inputSplitClass;
  private final byte[] inputSplitBytes;

  private transient InputSplit inputSplit;

  public HadoopSplit(
      HadoopSource<K, V> source,
      InputSplit inputSplit) {

    this.source = source;
    this.locations = ExceptionUtils.unchecked(() ->
        new HashSet<>(Arrays.asList(inputSplit.getLocations())));
    this.inputSplitClass = inputSplit.getClass();
    this.inputSplitBytes = serializeSplit(inputSplit, source.getConf());
  }

  @Override
  public Set<String> getLocations() {
    return locations;
  }

  @Override
  public BoundedReader<Pair<K, V>> openReader() throws IOException {
    try {
      final InputSplit inputSplit = getInputSplit();
      final Job job = source.newJob();
      final TaskAttemptContext ctx =
          new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
      final RecordReader<K, V> reader = source
          .newInputFormatClass()
          .createRecordReader(inputSplit, ctx);
      reader.initialize(inputSplit, ctx);
      return new HadoopReader<>(
          reader, source.getKeyClass(), source.getValueClass(), source.getConf());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while opening reader.");
    }
  }

  @Override
  public String toString() {
    return getClass().getName() + "<" + getInputSplit() + ">";
  }

  @SuppressWarnings("unchecked")
  private InputSplit getInputSplit() {
    if (inputSplit == null) {

      Deserializer<InputSplit> deserializer;
      deserializer = (Deserializer) new SerializationFactory(source.getConf())
          .getDeserializer(inputSplitClass);

      inputSplit = ExceptionUtils.unchecked(() -> {
        try (InputStream in = new ByteArrayInputStream(inputSplitBytes)) {
          deserializer.open(in);
          InputSplit ret = deserializer.deserialize(inputSplitClass.newInstance());
          deserializer.close();
          return ret;
        }
      });
    }
    return inputSplit;
  }

  private static byte[] serializeSplit(InputSplit inputSplit, Configuration conf) {
    @SuppressWarnings("unchecked")
    Serializer<InputSplit> serializer = (Serializer) new SerializationFactory(conf)
        .getSerializer(inputSplit.getClass());

    return ExceptionUtils.unchecked(() -> {
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        serializer.open(baos);
        serializer.serialize(inputSplit);
        serializer.close();
        return baos.toByteArray();
      }
    });
  }

  /**
   * Wraps Hadoop {@link RecordReader}
   */
  static class HadoopReader<K, V>
      extends AbstractIterator<Pair<K, V>>
      implements BoundedReader<Pair<K, V>> {

    private final RecordReader<K, V> reader;
    private final Cloner<K> keyCloner;
    private final Cloner<V> valueCloner;

    HadoopReader(
        RecordReader<K, V> reader,
        Class<K> keyClass, Class<V> valueClass,
        Configuration conf) {
      this.reader = Objects.requireNonNull(reader);
      // TODO: better
      this.keyCloner = Objects.requireNonNull(Cloner.get(keyClass, conf));
      this.valueCloner = Objects.requireNonNull(Cloner.get(valueClass, conf));
    }

    @Override
    protected Pair<K, V> computeNext() {
      return ExceptionUtils.unchecked(() -> {
        if (reader.nextKeyValue()) {
          final K key = reader.getCurrentKey();
          final V value = reader.getCurrentValue();
          // ~ clone key values since they are reused
          // between calls to RecordReader#nextKeyValue
          return Pair.of(keyCloner.clone(key), valueCloner.clone(value));
        } else {
          return endOfData();
        }
      });
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

}
