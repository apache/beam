package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Objects;

public class HadoopOutputFormatAdapter implements DataSink<Pair<?,?>> {

  private final Class<? extends OutputFormat> hadoopFormatCls;
  private final SerializableWritable<Configuration> conf;

  private transient OutputFormat<?, ?> hadoopFormatInstance;

  public HadoopOutputFormatAdapter(Class<? extends OutputFormat> hadoopFormatCls,
                                   SerializableWritable<Configuration> conf)
  {
    this.hadoopFormatCls = Objects.requireNonNull(hadoopFormatCls);
    this.conf = Objects.requireNonNull(conf);
  }

  @Override
  @SneakyThrows
  public void initialize() {
    OutputCommitter committer = getHadoopFormatInstance()
            .getOutputCommitter(
                    HadoopUtils.createTaskContext(conf.getWritable(), 0));

    committer.setupJob(HadoopUtils.createJobContext(conf.getWritable()));
  }

  @Override
  @SneakyThrows
  public Writer<Pair<?, ?>> openWriter(int partitionId) {
    TaskAttemptContext ctx =
            HadoopUtils.createTaskContext(conf.getWritable(), partitionId);

    RecordWriter<?, ?> writer =
            getHadoopFormatInstance().getRecordWriter(ctx);

    OutputCommitter committer =
            getHadoopFormatInstance().getOutputCommitter(ctx);

    return new HadoopWriter(writer, committer, ctx);
  }

  /**
   * Retrieves the instance or create new if not exists.
   */
  private OutputFormat<?, ?> getHadoopFormatInstance()
          throws InstantiationException, IllegalAccessException
  {
    if (hadoopFormatInstance == null) {
      hadoopFormatInstance = HadoopUtils.instantiateHadoopFormat(
              hadoopFormatCls,
              OutputFormat.class,
              conf.getWritable());
    }

    return hadoopFormatInstance;
  }

  @Override
  @SneakyThrows
  public void commit() throws IOException {
    OutputCommitter committer = getHadoopFormatInstance()
            .getOutputCommitter(
                    HadoopUtils.createTaskContext(conf.getWritable(), 0));

    committer.commitJob(HadoopUtils.createJobContext(conf.getWritable()));
  }

  @Override
  @SneakyThrows
  public void rollback() {
    OutputCommitter committer = getHadoopFormatInstance()
            .getOutputCommitter(
                    HadoopUtils.createTaskContext(conf.getWritable(), 0));

    committer.abortJob(HadoopUtils.createJobContext(
            conf.getWritable()), JobStatus.State.FAILED);
  }

  /**
   * Wraps Hadoop {@link RecordWriter}
   */
  private static class HadoopWriter implements Writer<Pair<?, ?>> {

    private final RecordWriter hadoopWriter;
    private final OutputCommitter hadoopCommitter;
    private final TaskAttemptContext ctx;

    public HadoopWriter(RecordWriter hadoopWriter,
                        OutputCommitter committer,
                        TaskAttemptContext ctx)
    {
      this.hadoopWriter = Objects.requireNonNull(hadoopWriter);
      this.hadoopCommitter = Objects.requireNonNull(committer);
      this.ctx = ctx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(Pair<?, ?> record) throws IOException {
      try {
        hadoopWriter.write(record.getKey(), record.getValue());
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void commit() throws IOException {
      hadoopCommitter.commitTask(ctx);
    }

    @Override
    public void rollback() throws IOException {
      hadoopCommitter.abortTask(ctx);
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }
}
