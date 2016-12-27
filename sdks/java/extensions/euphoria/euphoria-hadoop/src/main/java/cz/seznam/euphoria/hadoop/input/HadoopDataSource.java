package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import cz.seznam.euphoria.hadoop.utils.Cloner;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HadoopDataSource<K, V> implements DataSource<Pair<K, V>> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
  private final SerializableWritable<Configuration> conf;

  private transient InputFormat<K, V> hadoopFormatInstance;

  public HadoopDataSource(Class<K> keyClass, Class<V> valueClass,
      Class<? extends InputFormat<K, V>> hadoopFormatCls,
      SerializableWritable<Configuration> conf) {

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
    this.conf = Objects.requireNonNull(conf);
  }

  @Override
  @SneakyThrows
  public List<Partition<Pair<K, V>>> getPartitions() {
    Configuration c = conf.getWritable();
    return getHadoopFormatInstance()
            .getSplits(HadoopUtils.createJobContext(c))
            .stream()
            .map(split -> new HadoopPartition<>(keyClass, valueClass, hadoopFormatCls, conf, split))
            .collect(Collectors.toList());

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
      implements Reader<Pair<K, V>> {

    private final RecordReader<?, ?> hadoopReader;
    private final Cloner<K> keyCloner;
    private final Cloner<V> valueCloner;

    public HadoopReader(
        RecordReader<?, ?> hadoopReader,
        Class<K> keyClass, Class<V> valueClass,
        Configuration conf) {

      this.hadoopReader = Objects.requireNonNull(hadoopReader);
      this.keyCloner = Objects.requireNonNull(Cloner.get(keyClass, conf));
      this.valueCloner = Objects.requireNonNull(Cloner.get(valueClass, conf));
    }

    @Override
    @SneakyThrows
    protected Pair<K, V> computeNext() {
      if (hadoopReader.nextKeyValue()) {
        K key = (K) hadoopReader.getCurrentKey();
        V value = (V) hadoopReader.getCurrentValue();

        // ~ clone key values since they are reused
        // between calls to RecordReader#nextKeyValue
        return Pair.of(keyCloner.clone(key), valueCloner.clone(value));
      } else {
        return endOfData();
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
  private static class HadoopPartition<K, V> implements Partition<Pair<K, V>> {

    private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
    private SerializableWritable<Configuration> conf;
    private Set<String> locations;
    private final byte[] hadoopSplit; // ~ serialized
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public HadoopPartition(Class<K> keyClass, Class<V> valueClass,
                           Class<? extends InputFormat<K, V>> hadoopFormatCls,
                           SerializableWritable<Configuration> conf,
                           InputSplit hadoopSplit) {

      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
      this.conf = Objects.requireNonNull(conf);
      this.locations = Arrays.stream(hadoopSplit.getLocations())
              .collect(Collectors.toSet());
      this.hadoopSplit = HadoopUtils.serializeToBytes(hadoopSplit);
    }

    @Override
    public Set<String> getLocations() {
      return locations;
    }

    @Override
    @SneakyThrows
    public Reader<Pair<K, V>> openReader() throws IOException {
      InputSplit hadoopSplit =
              (InputSplit) HadoopUtils.deserializeFromBytes(this.hadoopSplit);
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

    }
  }

}
