package cz.seznam.euphoria.hadoop.input;

import cz.seznam.euphoria.hadoop.SerializableWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class SequenceFileDataSourceFactory<K extends Writable, V extends Writable> {

  public HadoopDataSource<K, V> get(Path path) {

    Configuration conf = new Configuration();
    conf.set(FileInputFormat.INPUT_DIR, path.toString());

    return
      new HadoopDataSource<>(
        (Class) SequenceFileInputFormat.class,
        new SerializableWritable<>(conf)
      );
  }
}
