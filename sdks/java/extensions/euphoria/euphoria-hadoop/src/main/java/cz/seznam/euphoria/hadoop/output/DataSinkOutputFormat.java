
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;

import cz.seznam.euphoria.hadoop.utils.Serializer;
import java.io.IOException;
import java.util.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@code OutputFormat} created from {@code DataSink}.
 * Because of the hadoop output format contract, we need to be able to
 * instantiate the format from {@code Class} object, therefore we
 * need to serialize the underlying {@code DataSink} to bytes and
 * store in to configuration.
 */
public class DataSinkOutputFormat<V> extends OutputFormat<NullWritable, V> {

  private static final String DATA_SINK = "cz.seznam.euphoria.hadoop.data-sink-serialized";

  /**
   * Sets given {@link DataSink} into Hadoop configuration. Note that original
   * configuration is modified.
   * @param conf Instance of Hadoop configuration
   * @param sink Euphoria sink
   * @return Modified configuration
   */
  public static <V> Configuration configure(
      Configuration conf, DataSink<V> sink) throws IOException {
    
    conf.set(DATA_SINK, toBase64(sink));
    return conf;
  }

  private static class HadoopRecordWriter<V> extends RecordWriter<NullWritable, V> {

    private final Writer<V> writer;

    @SuppressWarnings("unchecked")
    HadoopRecordWriter(Writer<V> writer) {
      this.writer = writer;
    }

    @Override
    public void write(NullWritable k, V v) throws IOException {
      writer.write(v);
    }

    @Override
    public void close(TaskAttemptContext tac) throws IOException {
      writer.flush();
    }

  }

  private static String toBase64(DataSink<?> sink) throws IOException {

    byte[] bytes = Serializer.toBytes(sink);
    return Base64.getEncoder().encodeToString(bytes);
  }

  @SuppressWarnings("unchecked")
  private static <V> DataSink<V> fromBase64(String base64Bytes)
      throws IOException, ClassNotFoundException {

    byte[] bytes = Base64.getDecoder().decode(base64Bytes);
    return Serializer.fromBytes(bytes);
  }

  // instance of the data sink
  private DataSink<V> sink;
  // the single writer per output format instance and thread
  private Writer<V> writer;

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext tac)
      throws IOException, InterruptedException {

    instantiateWriter(tac);
    return new HadoopRecordWriter<>(writer);
  }

  @Override
  public void checkOutputSpecs(JobContext jc)
      throws IOException, InterruptedException {
    // just try to get the sink
    instantiateSink(jc);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext tac)
      throws IOException, InterruptedException {
    
    instantiateSink(tac);
    return new OutputCommitter() {

      @Override
      public void setupJob(JobContext jc) throws IOException {
        instantiateSink(jc);
      }

      @Override
      public void setupTask(TaskAttemptContext tac) throws IOException {
        instantiateWriter(tac);
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return true;
      }

      @Override
      public void commitTask(TaskAttemptContext tac) throws IOException {
        if (writer != null) {
          writer.commit();
          writer.close();
        }
      }

      @Override
      public void abortTask(TaskAttemptContext tac) throws IOException {
        if (writer != null) {
          writer.rollback();
          writer.close();
        }
      }

      @Override
      public void commitJob(JobContext jobContext) throws IOException {
        super.commitJob(jobContext);
        sink.commit();
      }

      @Override
      public void abortJob(JobContext jobContext, JobStatus.State state)
          throws IOException {        
        super.abortJob(jobContext, state);
        sink.rollback();
      }

    };
  }


  private void instantiateWriter(TaskAttemptContext tac) throws IOException {
    if (writer == null) {
      instantiateSink(tac);
      writer = sink.openWriter(tac.getTaskAttemptID().getTaskID().getId());
    }
  }

  private void instantiateSink(JobContext jc) throws IOException {
    if (sink == null) {
      String sinkBytes = jc.getConfiguration().get(DATA_SINK, null);
      if (sinkBytes == null) {
        throw new IllegalStateException(
            "Invalid output spec, call `DataSinkOutputFormat#configure` before passing "
                + " the configuration to output");
      }
      try {
        sink = fromBase64(sinkBytes);
        sink.initialize();
      } catch (ClassNotFoundException ex) {
        throw new IOException(ex);
      }
    }
  }

}
