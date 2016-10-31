
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.utils.Serializer;
import java.io.IOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * {@code OutputFormat} created from {@code DataSink}.
 * Because of the hadoop outputformat contract, we need to be able to
 * instantiate the format from {@code Class} object, therefore we
 * need to serialize the underlying {@code DataSink} to bytes and
 * store in to configuration.
 */
public class DataSinkOutputFormat<K, V> extends OutputFormat<K, V> {

  private static final String DATA_SINK = "cz.seznam.euphoria.hadoop.data-sink-serialized";

  public static <K, V> void configure(
      Configuration conf, DataSink<Pair<K, V>> sink) throws IOException {
    
    conf.set(DATA_SINK, toBase64(sink));
  }

  private static class HadoopRecordWriter<K, V> extends RecordWriter<K, V> {

    private final Writer<Pair<K, V>> writer;

    @SuppressWarnings("unchecked")
    HadoopRecordWriter(Writer<? extends Pair<K, V>> writer) {
      this.writer = (Writer<Pair<K, V>>) writer;
    }

    @Override
    public void write(K k, V v) throws IOException {
      writer.write(Pair.of(k, v));
    }

    @Override
    public void close(TaskAttemptContext tac) throws IOException {
      writer.flush();
    }

  }

  private static String toBase64(DataSink<?> sink) throws IOException {

    byte[] bytes = Serializer.toBytes(sink);
    return Base64.encodeBase64String(bytes);
  }

  @SuppressWarnings("unchecked")
  private static <K, V> DataSink<? extends Pair<K, V>> fromBase64(String base64Bytes)
      throws IOException, ClassNotFoundException {

    byte[] bytes = Base64.decodeBase64(base64Bytes);
    return Serializer.fromBytes(bytes);
  }

  // instance of the data sink
  private DataSink<? extends Pair<K, V>> sink;
  // the single writer per outputformat instance and thread
  private Writer<? extends Pair<K, V>> writer;

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext tac)
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
