package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.DataSourceFactory;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.net.URI;

public class HadoopTextFileSourceFactory implements DataSourceFactory {

  @Override
  @SuppressWarnings("unchecked")
  public <T> DataSource<T> get(URI uri, Settings settings) {
    Configuration conf = HadoopUtils.createConfiguration(settings);

    // set input dir
    conf.set(FileInputFormat.INPUT_DIR, uri.toString());

    return (DataSource<T>) new HadoopInputFormatAdapter(
            TextInputFormat.class, new SerializableWritable<>(conf));
  }
}
