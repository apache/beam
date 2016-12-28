package cz.seznam.euphoria.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceFileDataSinkFactory<K extends Writable, V extends Writable> {

  public HadoopDataSink<K, V> get(Class<K> keyCls, Class<V> valueCls, Path path) {

    Configuration conf = new Configuration();
    conf.set(FileOutputFormat.OUTDIR, path.toString());
    conf.set(JobContext.OUTPUT_KEY_CLASS, keyCls.getName());
    conf.set(JobContext.OUTPUT_VALUE_CLASS, valueCls.getName());

    return new HadoopDataSink<>(
            (Class) SequenceFileOutputFormat.class,
            conf);
  }
}
