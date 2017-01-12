package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.net.URI;
import java.util.Objects;

/**
 * A data source reading hadoop based inputs as a sequences of text lines.
 */
public class HadoopTextFileDataSource extends HadoopDataSource<LongWritable, Text> {

  /**
   * A standard URI based factory for instances of {@link HadoopTextFileDataSource}.
   */
  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      return (DataSource<T>) new HadoopTextFileDataSource(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }


  /**
   * Convenience constructor invoking
   * {@link #HadoopTextFileDataSource(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @throws NullPointerException if the given path is {@code null}
   */
  public HadoopTextFileDataSource(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data source based on hadoop's {@link TextInputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  public HadoopTextFileDataSource(String path, Configuration hadoopConfig) {
    super(LongWritable.class, Text.class, TextInputFormat.class, hadoopConfig);
    hadoopConfig.set(TextInputFormat.INPUT_DIR, Objects.requireNonNull(path));
  }
}
