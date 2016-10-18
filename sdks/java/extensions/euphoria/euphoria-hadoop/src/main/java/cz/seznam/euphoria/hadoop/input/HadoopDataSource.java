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
import java.util.ArrayList;
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
  private final int maxSplits;

  private transient InputFormat<K, V> hadoopFormatInstance;

  public HadoopDataSource(Class<K> keyClass, Class<V> valueClass,
      Class<? extends InputFormat<K, V>> hadoopFormatCls,
      SerializableWritable<Configuration> conf) {
    
    this(keyClass, valueClass, hadoopFormatCls, conf, -1);
  }


  public HadoopDataSource(Class<K> keyClass, Class<V> valueClass,
      Class<? extends InputFormat<K, V>> hadoopFormatCls,
      SerializableWritable<Configuration> conf,
      int maxSplits) {

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
    this.conf = Objects.requireNonNull(conf);
    this.maxSplits = maxSplits;
  }

  @Override
  @SneakyThrows
  public List<Partition<Pair<K, V>>> getPartitions() {
    Configuration c = conf.getWritable();
    List<InputSplit> splits = getHadoopFormatInstance().getSplits(
        HadoopUtils.createJobContext(c));
    if (maxSplits > 0 && splits.size() > maxSplits) {
      List<List<InputSplit>> merged = new ArrayList<>(maxSplits);
      for (int i = 0; i < maxSplits; i++) {
        merged.add(new ArrayList<>());
      }
      int j = 0;
      for (int i = 0; i < splits.size(); i++) {
        merged.get(j++).add(splits.get(i));
        j %= maxSplits;
      }
      return merged
          .stream()
          .map(s -> new HadoopPartition<>(keyClass, valueClass, hadoopFormatCls,
              conf, s.toArray(new InputSplit[] { })))
          .collect(Collectors.toList());
    }
    // do not merge splits
    return splits
            .stream()
            .map(split -> new HadoopPartition<>(
                keyClass, valueClass, hadoopFormatCls, conf,
                new InputSplit[] { split }))
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
  private static class HadoopMultiReader<K, V>
      extends AbstractIterator<Pair<K, V>>
      implements Reader<Pair<K, V>> {
    
    private final TaskAttemptContext ctx;
    private final InputFormat inputFormat;
    private final InputSplit[] inputSplits;
    private final Cloner<K> keyCloner;
    private final Cloner<V> valueCloner;

    private int nextReader = 0;
    private RecordReader<?, ?> currentReader;

    public HadoopMultiReader(
        TaskAttemptContext ctx,
        InputFormat inputFormat,
        InputSplit[] splits,
        Class<K> keyClass, Class<V> valueClass,
        Configuration conf) {

      this.ctx = Objects.requireNonNull(ctx);
      this.inputFormat = Objects.requireNonNull(inputFormat);
      this.inputSplits = Objects.requireNonNull(splits);
      this.keyCloner = Objects.requireNonNull(Cloner.get(keyClass, conf));
      this.valueCloner = Objects.requireNonNull(Cloner.get(valueClass, conf));
    }

    @Override
    @SneakyThrows
    protected Pair<K, V> computeNext() {
      if (currentReader == null || !currentReader.nextKeyValue()) {
        moveToNextReader();
      }
      if (currentReader != null) {

        K key = (K) currentReader.getCurrentKey();
        V value = (V) currentReader.getCurrentValue();

        // ~ clone key values since they are reused
        // between calls to RecordReader#nextKeyValue
        return Pair.of(keyCloner.clone(key), valueCloner.clone(value));
      } else {
        return endOfData();
      }
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
    }

    /**
     * Set {@code currentReader} to next valid reader or null if no valid
     * readers left. The returned reader is ready to be called {@code getCurrentKey}.
     */
    @SuppressWarnings("unchecked")
    private void moveToNextReader() throws IOException, InterruptedException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
      while (nextReader < inputSplits.length) {
        System.err.println(" ** moving to next reader " + nextReader);
        InputSplit split = inputSplits[nextReader++];
        RecordReader reader = inputFormat.createRecordReader(split, ctx);
        reader.initialize(split, ctx);
        if (reader.nextKeyValue()) {
          currentReader = reader;
          return;
        }
     }
      // no more iterators
      currentReader = null;
    }
  }

  /**
   * Wraps Hadoop {@link InputSplit}
   */
  private static class HadoopPartition<K, V> implements Partition<Pair<K, V>> {

    private final Class<? extends InputFormat<K, V>> hadoopFormatCls;
    private SerializableWritable<Configuration> conf;
    private Set<String> locations;
    private final byte[][] hadoopSplits; // ~ serialized
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public HadoopPartition(Class<K> keyClass, Class<V> valueClass,
                           Class<? extends InputFormat<K, V>> hadoopFormatCls,
                           SerializableWritable<Configuration> conf,
                           InputSplit[] hadoopSplits) {

      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
      this.conf = Objects.requireNonNull(conf);

      this.locations = Arrays.stream(hadoopSplits)
          .flatMap(is -> {
            try {
              return Arrays.stream(is.getLocations());
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          })
          .collect(Collectors.toSet());
      
      this.hadoopSplits = new byte[hadoopSplits.length][];
      for (int i = 0; i < this.hadoopSplits.length; i++) {
        this.hadoopSplits[i] = HadoopUtils.serializeToBytes(hadoopSplits[i]);
      }
    }

    @Override
    public Set<String> getLocations() {
      return locations;
    }

    @Override
    @SneakyThrows
    public Reader<Pair<K, V>> openReader() throws IOException {
      InputSplit[] splits = new InputSplit[hadoopSplits.length];
      for (int i = 0; i < hadoopSplits.length; i++) {
        splits[i] = (InputSplit) HadoopUtils.deserializeFromBytes(hadoopSplits[i]);
      }
      Configuration conf = this.conf.getWritable();
      TaskAttemptContext ctx = HadoopUtils.createTaskContext(conf, 0);
      @SuppressWarnings("unchecked")
      InputFormat inputFormat = HadoopUtils.instantiateHadoopFormat(
          hadoopFormatCls,
          InputFormat.class,
          conf);

      return new HadoopMultiReader<>(
          ctx, inputFormat, splits, keyClass, valueClass, conf);

    }
  }

}
