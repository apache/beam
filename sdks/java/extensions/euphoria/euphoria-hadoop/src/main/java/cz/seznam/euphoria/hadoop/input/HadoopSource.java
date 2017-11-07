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

import com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.BoundedPartition;
import cz.seznam.euphoria.core.client.io.BoundedReader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import cz.seznam.euphoria.hadoop.utils.Cloner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A general purpose data source based on top of hadoop input formats.
 *
 * @param <K> the type of record keys
 * @param <V> the type of record values
 */
public class HadoopSource<K, V> implements BoundedDataSource<Pair<K, V>> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
  private final SerializableWritable<Configuration> conf;

  @Nullable
  private transient InputFormat<K, V> hadoopFormatInstance;

  public HadoopSource(Class<K> keyClass, Class<V> valueClass,
                      Class<? extends InputFormat<K, V>> hadoopFormatCls,
                      Configuration hadoopConf) {

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
    this.conf = new SerializableWritable<>(Objects.requireNonNull(hadoopConf));
  }

  @Override
  public List<BoundedPartition<Pair<K, V>>> getPartitions() {
    try {
      Configuration c = conf.getWritable();
      return getHadoopFormatInstance()
          .getSplits(HadoopUtils.createJobContext(c))
          .stream()
          .map(split -> new HadoopPartition<>(keyClass, valueClass, hadoopFormatCls, conf, split))
          .collect(Collectors.toList());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean isBounded() {
    return true;
  }

 /**
   * Retrieves the instance or create new if not exists.
   * You must always pass a valid configuration object
   * or {@code NullPointerException} might be thrown.
   */
  @SuppressWarnings("unchecked")
  private InputFormat<K, V> getHadoopFormatInstance()
          throws InstantiationException, IllegalAccessException
  {
    if (hadoopFormatInstance == null) {
      hadoopFormatInstance = HadoopUtils.instantiateHadoopFormat(
              hadoopFormatCls,
              InputFormat.class,
              conf.getWritable());
    }

    return hadoopFormatInstance;
  }

  /**
   * Wraps Hadoop {@link RecordReader}
   */
  private static class HadoopReader<K, V>
      extends AbstractIterator<Pair<K, V>>
      implements BoundedReader<Pair<K, V>> {

    private final RecordReader<K, V> hadoopReader;
    private final Cloner<K> keyCloner;
    private final Cloner<V> valueCloner;

    public HadoopReader(
        RecordReader<K, V> hadoopReader,
        Class<K> keyClass, Class<V> valueClass,
        Configuration conf) {

      this.hadoopReader = Objects.requireNonNull(hadoopReader);
      this.keyCloner = Objects.requireNonNull(Cloner.get(keyClass, conf));
      this.valueCloner = Objects.requireNonNull(Cloner.get(valueClass, conf));
    }

    @Override
    protected Pair<K, V> computeNext() {
      try {
        if (hadoopReader.nextKeyValue()) {
          K key = hadoopReader.getCurrentKey();
          V value = hadoopReader.getCurrentValue();

          // ~ clone key values since they are reused
          // between calls to RecordReader#nextKeyValue
          return Pair.of(keyCloner.clone(key), valueCloner.clone(value));
        } else {
          return endOfData();
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void close() throws IOException {
      hadoopReader.close();
    }
  }

  /**
   * Wraps Hadoop {@link InputSplit}
   */
  private static class HadoopPartition<K, V> implements BoundedPartition<Pair<K, V>> {

    private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
    private SerializableWritable<Configuration> conf;
    private Set<String> locations;
    private final byte[] hadoopSplit; // ~ serialized
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    private transient InputSplit hadoopSplitDeserialized;

    @SuppressWarnings("unchecked")
    public HadoopPartition(Class<K> keyClass, Class<V> valueClass,
                           Class<? extends InputFormat<K, V>> hadoopFormatCls,
                           SerializableWritable<Configuration> conf,
                           InputSplit hadoopSplit) {

      try {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
        this.conf = Objects.requireNonNull(conf);
        this.locations = Arrays.stream(hadoopSplit.getLocations())
            .collect(Collectors.toSet());
        this.hadoopSplit = HadoopUtils.serializeToBytes(hadoopSplit);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Set<String> getLocations() {
      return locations;
    }

    @Override
    public BoundedReader<Pair<K, V>> openReader() throws IOException {
      try {
        InputSplit hadoopSplit = getHadoopInputSplit();
        Configuration conf = this.conf.getWritable();
        TaskAttemptContext ctx = HadoopUtils.createTaskContext(conf, 0);
        @SuppressWarnings("unchecked")
            RecordReader<K, V> reader =
            HadoopUtils.instantiateHadoopFormat(
                hadoopFormatCls,
                InputFormat.class,
                conf)
                .createRecordReader(hadoopSplit, ctx);

        reader.initialize(hadoopSplit, ctx);

        return new HadoopReader<>(reader, keyClass, valueClass, conf);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    private InputSplit getHadoopInputSplit() {
      if (hadoopSplitDeserialized == null) {
        hadoopSplitDeserialized =
            (InputSplit) HadoopUtils.deserializeFromBytes(this.hadoopSplit);
      }
      return hadoopSplitDeserialized;
    }

    @Override
    public String toString() {
      return getClass().getName() + "<" +  getHadoopInputSplit() + ">";
    }
  }

}
