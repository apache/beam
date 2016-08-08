package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSinkFactory;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class HadoopTextFileSinkFactory implements DataSinkFactory {

  @Override
  @SuppressWarnings("unchecked")
  public <T> DataSink<T> get(URI uri, Settings settings) {
    Configuration conf = HadoopUtils.createConfiguration(settings);

    // set input dir
    conf.set(FileOutputFormat.OUTDIR, uri.toString());

    return (DataSink<T>) new HadoopDataSink(
            TextOutputFormat.class, new SerializableWritable<>(conf));
  }
}
