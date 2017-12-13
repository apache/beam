/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.hadoop.output;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.ExceptionUtils;
import cz.seznam.euphoria.hadoop.HadoopUtils;
import cz.seznam.euphoria.hadoop.SerializableWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Objects;

public class HadoopSink<K, V> implements DataSink<Pair<K, V>> {

  private final Class<? extends OutputFormat<K, V>> outputFormatClass;
  private final SerializableWritable<Configuration> conf;
  private final SerializableWritable<JobID> jobID;

  public HadoopSink(Class<? extends OutputFormat<K, V>> outputFormatClass,
                    Configuration conf) {
    this.outputFormatClass = Objects.requireNonNull(outputFormatClass);
    this.conf = new SerializableWritable<>(Objects.requireNonNull(conf));
    this.jobID = new SerializableWritable<>(HadoopUtils.getJobID());
  }

  @Override
  public void initialize() {
    ExceptionUtils.unchecked(() -> {
      final TaskAttemptContext setupContext =
          HadoopUtils.createSetupTaskContext(conf.get(), jobID.get());
      newOutputFormatInstance()
          .getOutputCommitter(setupContext)
          .setupJob(setupContext);
    });
  }

  @Override
  public HadoopWriter<K, V> openWriter(int partitionId) {
    try {
      final OutputFormat<K, V> outputFormat = newOutputFormatInstance();
      final TaskAttemptContext taskContext =
          HadoopUtils.createTaskContext(conf.get(), jobID.get(), partitionId);
      return new HadoopWriter<>(
          outputFormat.getRecordWriter(taskContext),
          outputFormat.getOutputCommitter(taskContext),
          taskContext);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Retrieves the instance or create new if not exists.
   */
  private OutputFormat<K, V> newOutputFormatInstance() {
    return ExceptionUtils.unchecked(() -> {
      final OutputFormat<K, V> outputFormat = outputFormatClass.newInstance();
      if (outputFormat instanceof Configurable) {
        ((Configurable) outputFormat).setConf(conf.get());
      }
      return outputFormat;
    });
  }

  @Override
  public void commit() throws IOException {
    try {
      final TaskAttemptContext cleanupContext =
          HadoopUtils.createCleanupTaskContext(conf.get(), jobID.get());
      newOutputFormatInstance()
          .getOutputCommitter(cleanupContext)
          .commitJob(cleanupContext);
    } catch (Exception e) {
      throw new IOException("Unable to commit output", e);
    }
  }

  @Override
  public void rollback() throws IOException {
    try {
      final TaskAttemptContext cleanupContext =
          HadoopUtils.createCleanupTaskContext(conf.get(), jobID.get());
      newOutputFormatInstance()
          .getOutputCommitter(cleanupContext)
          .abortJob(cleanupContext, JobStatus.State.FAILED);
    } catch (Exception e) {
      throw new IOException("Unable to rollback output", e);
    }
  }

  /**
   * Retrieve configuration.
   * @return the configuration used in this source
   */
  public Configuration getConfiguration() {
    return conf.get();
  }

  /**
   * Wraps Hadoop {@link RecordWriter}
   */
  public static class HadoopWriter<K, V> implements Writer<Pair<K, V>> {

    private final RecordWriter<K, V> hadoopWriter;
    private final OutputCommitter hadoopCommitter;
    private final TaskAttemptContext ctx;

    public HadoopWriter(RecordWriter<K, V> hadoopWriter,
                        OutputCommitter committer,
                        TaskAttemptContext ctx) {
      this.hadoopWriter = Objects.requireNonNull(hadoopWriter);
      this.hadoopCommitter = Objects.requireNonNull(committer);
      this.ctx = ctx;
    }

    @Override
    public void write(Pair<K, V> record) throws IOException {
      try {
        hadoopWriter.write(record.getFirst(), record.getSecond());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while writing!");
      }
    }

    @Override
    public void commit() throws IOException {
      try {
        // flush pending changes - if any
        hadoopWriter.close(ctx);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted from closing hadoop writer!");
      }
      // task is complete - we can commit it
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

    public RecordWriter<K, V> getRecordWriter() {
      return hadoopWriter;
    }

    public OutputCommitter getOutputCommitter() {
      return hadoopCommitter;
    }

    public TaskAttemptContext getTaskAttemptContext() {
      return ctx;
    }
  }
}
