package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

/**
 * A convenience data sink based on hadoop's {@link TextOutputFormat}.
 *
 * @param <K> the type of keys emitted
 * @param <V> the type of values emitted
 */
public class HadoopTextFileDataSink<K, V> extends HadoopDataSink<K, V> {

  /**
   * A standard URI based factory for instance of {@link HadoopTextFileDataSink}.
   */
  public static final class Factory implements DataSinkFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSink<T> get(URI uri, Settings settings) {
      return (DataSink<T>) new HadoopTextFileDataSink(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }

  /**
   * Convenience constructor invoking {@link #HadoopTextFileDataSink(String, Configuration)}
   * with a newly created hadoop configuration.
   *
   * @param path the path where to place the output to
   *
   * @throws NullPointerException if any of the given parameters is {@code null}
   */
  public HadoopTextFileDataSink(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data sink based on hadoop's {@link TextOutputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public HadoopTextFileDataSink(String path, Configuration hadoopConfig) {
    super((Class) TextOutputFormat.class, hadoopConfig);
    hadoopConfig.set(FileOutputFormat.OUTDIR, path);
  }
}
