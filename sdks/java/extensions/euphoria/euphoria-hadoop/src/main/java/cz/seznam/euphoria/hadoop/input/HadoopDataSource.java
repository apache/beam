package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
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

public class HadoopDataSource<K extends Writable, V extends Writable>
    implements DataSource<Pair<K, V>> {

  private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
  private final SerializableWritable<Configuration> conf;

  private transient InputFormat<K, V> hadoopFormatInstance;

  public HadoopDataSource(Class<? extends InputFormat<K, V>> hadoopFormatCls,
                          SerializableWritable<Configuration> conf)
  {
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
            .map(split -> new HadoopPartition<>(hadoopFormatCls, conf, split))
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
  private static class HadoopReader<K extends Writable, V extends Writable>
          extends AbstractIterator<Pair<K, V>>
          implements Reader<Pair<K, V>>
  {
    private final RecordReader<?, ?> hadoopReader;
    private final Configuration conf;

    public HadoopReader(RecordReader<?, ?> hadoopReader, Configuration conf) {
      this.hadoopReader = Objects.requireNonNull(hadoopReader);
      this.conf = Objects.requireNonNull(conf);
    }

    @Override
    @SneakyThrows
    protected Pair<K, V> computeNext() {
      if (hadoopReader.nextKeyValue()) {

        // ~ cast to Writable (may throw ClassCastException
        // when input format doesn't support Writables)
        K key = (K) hadoopReader.getCurrentKey();
        V value = (V) hadoopReader.getCurrentValue();

        // ~ clone Writables since they are reused
        // between calls to RecordReader#nextKeyValue
        return Pair.of(WritableUtils.clone(key, conf), WritableUtils.clone(value, conf));
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
  private static class HadoopPartition<K extends Writable, V extends Writable>
      implements Partition<Pair<K, V>> {

    private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
    private SerializableWritable<Configuration> conf;
    private Set<String> locations;
    private final byte [] hadoopSplit; // ~ serialized

    @SneakyThrows
    public HadoopPartition(Class<? extends InputFormat<K, V>> hadoopFormatCls,
                           SerializableWritable<Configuration> conf,
                           InputSplit hadoopSplit)
    {
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

      return new HadoopReader<>(reader, conf);
    }
  }

}
