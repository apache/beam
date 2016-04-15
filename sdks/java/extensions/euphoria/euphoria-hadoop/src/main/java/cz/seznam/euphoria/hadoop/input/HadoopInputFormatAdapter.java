package cz.seznam.euphoria.hadoop.input;

import com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.operator.Pair;
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

class HadoopInputFormatAdapter implements DataSource<Pair<?, ?>> {

  private final Class<? extends InputFormat> hadoopFormatCls;
  private final SerializableWritable<Configuration> conf;

  private transient InputFormat<?, ?> hadoopFormatInstance;

  public HadoopInputFormatAdapter(Class<? extends InputFormat> hadoopFormatCls,
                                  SerializableWritable<Configuration> conf)
  {
    this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
    this.conf = Objects.requireNonNull(conf);
  }

  @Override
  @SneakyThrows
  public List<Partition<Pair<?, ?>>> getPartitions() {
    Configuration c = conf.getWritable();
    return getHadoopFormatInstance()
            .getSplits(HadoopUtils.createJobContext(c))
            .stream()
            .map(split -> new HadoopPartition(hadoopFormatCls, c, split))
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
  private InputFormat<?, ?> getHadoopFormatInstance()
          throws InstantiationException, IllegalAccessException
  {
    if (hadoopFormatInstance == null) {
      hadoopFormatInstance = HadoopUtils.instantiateHadoopFormat(
              hadoopFormatCls,
              conf.getWritable());
    }

    return hadoopFormatInstance;
  }

  /**
   * Wraps Hadoop {@link RecordReader}
   */
  private static class HadoopReader
          extends AbstractIterator<Pair<?, ?>>
          implements Reader<Pair<?, ?>>
  {
    private final RecordReader<?, ?> hadoopReader;

    public HadoopReader(RecordReader<?, ?> hadoopReader) {
      this.hadoopReader = Objects.requireNonNull(hadoopReader);
    }

    @Override
    @SneakyThrows
    protected Pair<?, ?> computeNext() {
      return hadoopReader.nextKeyValue() ?
              Pair.of(hadoopReader.getCurrentKey(), hadoopReader.getCurrentValue()) :
              endOfData();
    }

    @Override
    public void close() throws IOException {
      hadoopReader.close();
    }
  }

  /**
   * Wraps Hadoop {@link InputSplit}
   */
  private static class HadoopPartition implements Partition<Pair<?, ?>> {

    private final Class<? extends InputFormat> hadoopFormatCls;
    private final Configuration conf;
    private final InputSplit hadoopSplit;

    public HadoopPartition(Class<? extends InputFormat> hadoopFormatCls,
                           Configuration conf,
                           InputSplit hadoopSplit)
    {
      this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
      this.conf = Objects.requireNonNull(conf);
      this.hadoopSplit = Objects.requireNonNull(hadoopSplit);
    }

    @Override
    @SneakyThrows
    public Set<String> getLocations() {
      return Arrays.stream(hadoopSplit.getLocations())
              .collect(Collectors.toSet());
    }

    @Override
    @SneakyThrows
    public Reader<Pair<?, ?>> openReader() throws IOException {
      TaskAttemptContext ctx = HadoopUtils.createTaskContext(conf);
      RecordReader<?, ?> reader =
              HadoopUtils.instantiateHadoopFormat(hadoopFormatCls, conf)
                      .createRecordReader(hadoopSplit, ctx);

      reader.initialize(hadoopSplit, ctx);

      return new HadoopReader(reader);
    }
  }

}
