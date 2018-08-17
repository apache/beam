package cz.seznam.euphoria.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * A convenience data sink to produce output through hadoop's sequence file for a
 * specified key and value type.
 *
 * @param <K> the type of the keys emitted
 * @param <V> the type of the values emitted
 */
public class SequenceFileDataSink<K, V> extends HadoopDataSink<K, V> {

  // TODO provide factory: how to parse out the keyType and valueType info from the URI?

  /**
   * Convenience constructor delegating to
   * {@link #SequenceFileDataSink(Class, Class, String, Configuration)} with a newly
   * created hadoop configuration.
   *
   * @param keyType the type of the key consumed and emitted to the output
   *         by the new data sink
   * @param valueType the type of the value consumed and emitted to the output
   *         by the new data sink
   * @param path the destination where to place the output to
   */
  public SequenceFileDataSink(Class<K> keyType, Class<V> valueType, String path) {
    this(keyType, valueType, path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link SequenceFileOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration as well as the key and value types.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SequenceFileDataSink(Class<K> keyType, Class<V> valueType,
                              String path, Configuration hadoopConfig) {
    super((Class) SequenceFileOutputFormat.class, hadoopConfig);
    hadoopConfig.set(FileOutputFormat.OUTDIR, path);
    hadoopConfig.set(JobContext.OUTPUT_KEY_CLASS, keyType.getName());
    hadoopConfig.set(JobContext.OUTPUT_VALUE_CLASS, valueType.getName());
  }
}
