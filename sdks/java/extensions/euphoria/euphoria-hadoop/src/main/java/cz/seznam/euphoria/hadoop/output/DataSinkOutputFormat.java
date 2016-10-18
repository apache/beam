
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.xerces.impl.dv.util.Base64;

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
      writer.close();
    }

  }

  private static String toBase64(DataSink<?> sink) throws IOException {
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(sink);
    oos.close();
    return Base64.encode(baos.toByteArray());
  }

  @SuppressWarnings("unchecked")
  private static <K, V> DataSink<? extends Pair<K, V>> fromBase64(String base64Bytes)
      throws IOException, ClassNotFoundException {

    byte[] bytes = Base64.decode(base64Bytes);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    return (DataSink<? extends Pair<K, V>>) ois.readObject();
  }



  // instance of the data sink
  private DataSink<? extends Pair<K, V>> sink;
  // the single writer per outputformat instance
  private Writer<? extends Pair<K, V>> writer;

  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext tac)
      throws IOException, InterruptedException {

    instantiateWriter(tac);
    return new HadoopRecordWriter<>(writer);
  }

  public void instantiateWriter(TaskAttemptContext tac) throws IOException {
    if (writer == null) {
      instantiateSink(tac);
      writer = sink.openWriter(tac.getTaskAttemptID().getId());
    }
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
    instantiateWriter(tac);
    return new OutputCommitter() {

      @Override
      public void setupJob(JobContext jc) throws IOException {
        // nop
      }

      @Override
      public void setupTask(TaskAttemptContext tac) throws IOException {
        // nop
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return true;
      }

      @Override
      public void commitTask(TaskAttemptContext tac) throws IOException {
        writer.commit();
      }

      @Override
      public void abortTask(TaskAttemptContext tac) throws IOException {
        writer.rollback();
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

  private void instantiateSink(JobContext jc) throws IOException {
    String sinkBytes = jc.getConfiguration().get(DATA_SINK, null);
    if (sinkBytes == null) {
      throw new IllegalStateException(
          "Invalid output spec, call `DataSinkOutputFormat#configure` before passing "
              + " the configuration to output");
    }
    try {
      sink = fromBase64(sinkBytes);
    } catch (ClassNotFoundException ex) {
      throw new IOException(ex);
    }
  }

}
