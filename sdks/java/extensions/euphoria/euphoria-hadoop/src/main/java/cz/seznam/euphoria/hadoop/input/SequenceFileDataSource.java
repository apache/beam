package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.net.URI;

/**
 * A data source reading hadoop based inputs as a sequences of records persisted in
 * (binary) sequence files.
 */
public class SequenceFileDataSource<K extends Writable, V extends Writable>
    extends HadoopDataSource<K, V>  {

  /**
   * A standard URI based factory for instances of {@link SequenceFileDataSource}.
   */
  public static final class Factory implements DataSourceFactory {
    @Override
    @SuppressWarnings("unchecked")
    public <T> DataSource<T> get(URI uri, Settings settings) {
      return (DataSource<T>) new SequenceFileDataSource<>(
          uri.toString(), HadoopUtils.createConfiguration(settings));
    }
  }


  /**
   * Convenience constructor invoking
   * {@link #SequenceFileDataSource(String, Configuration)} with a newly created
   * hadoop configuration.
   *
   * @throws NullPointerException if the given path is {@code null}
   */
  public SequenceFileDataSource(String path) {
    this(path, new Configuration());
  }

  /**
   * Constructs a data source based on hadoop's {@link SequenceFileInputFormat}.
   * The specified path is automatically set/overridden in the given hadoop
   * configuration.
   *
   * @param path the path to read data from
   * @param hadoopConfig the hadoop configuration to build on top of
   *
   * @throws NullPointerException if any of the parameters is {@code null}
   */
  @SuppressWarnings("unchecked")
  public SequenceFileDataSource(String path, Configuration hadoopConfig) {
    super((Class) Writable.class, (Class) Writable.class,
        (Class) SequenceFileInputFormat.class, hadoopConfig);
    hadoopConfig.set(FileInputFormat.INPUT_DIR, path);
  }

}
