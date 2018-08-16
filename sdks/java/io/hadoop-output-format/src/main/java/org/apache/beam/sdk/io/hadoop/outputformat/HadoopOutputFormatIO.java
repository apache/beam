/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.outputformat;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopOutputFormatIO} is a Transform for writing data to any sink which implements
 * Hadoop {@link OutputFormat}. For example - Cassandra, Elasticsearch, HBase, Redis, Postgres etc.
 * {@link HadoopOutputFormatIO} has to make several performance trade-offs in connecting to {@link
 * OutputFormat}, so if there is another Beam IO Transform specifically for connecting to your data
 * sink of choice, we would recommend using that one, but this IO Transform allows you to connect to
 * many data sinks that do not yet have a Beam IO Transform.
 *
 * <p>You will need to pass a Hadoop {@link Configuration} with parameters specifying how the write
 * will occur. Many properties of the Configuration are optional, and some are required for certain
 * {@link OutputFormat} classes, but the following properties must be set for all OutputFormats:
 *
 * <ul>
 *   <li>{@code mapreduce.job.outputformat.class}: The {@link OutputFormat} class used to connect to
 *       your data sink of choice.
 *   <li>{@code mapreduce.job.outputformat.key.class}: The key class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 *   <li>{@code mapreduce.job.outputformat.value.class}: The value class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 * </ul>
 *
 * For example:
 *
 * <pre>{@code
 * Configuration myHadoopConfiguration = new Configuration(false);
 * // Set Hadoop OutputFormat, key and value class in configuration
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.class&quot;,
 *    MyDbOutputFormatClass, OutputFormat.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.key.class&quot;,
 *    MyDbOutputFormatKeyClass, Object.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.value.class&quot;,
 *    MyDbOutputFormatValueClass, Object.class);
 * }</pre>
 *
 * <p>You will need to set appropriate OutputFormat key and value class (i.e.
 * "mapreduce.job.outputformat.key.class" and "mapreduce.job.outputformat.value.class") in Hadoop
 * {@link Configuration}. If you set different OutputFormat key or value class than OutputFormat's
 * actual key or value class then, it may result in an error like "unexpected extra bytes after
 * decoding" while the decoding process of key/value object happens. Hence, it is important to set
 * appropriate OutputFormat key and value class.
 *
 * <h3>Writing using {@link HadoopOutputFormatIO}</h3>
 *
 * <pre>{@code
 * Pipeline p = ...; // Create pipeline.
 * // Read data only with Hadoop configuration.
 * p.apply("read",
 *     HadoopOutputFormatIO.<OutputFormatKeyClass, OutputFormatKeyClass>write()
 *              .withConfiguration(myHadoopConfiguration);
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HadoopOutputFormatIO {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopOutputFormatIO.class);

  public static final String OUTPUTFORMAT_CLASS = "mapreduce.job.outputformat.class";
  public static final String OUTPUTFORMAT_KEY_CLASS = "mapreduce.job.outputformat.key.class";
  public static final String OUTPUTFORMAT_VALUE_CLASS = "mapreduce.job.outputformat.value.class";

  /**
   * Creates an uninitialized {@link HadoopOutputFormatIO.Write}. Before use, the {@code Write} must
   * be initialized with a HadoopOutputFormatIO.Write#withConfiguration(HadoopConfiguration) that
   * specifies the sink.
   */
  public static <K, V> Write<K, V> write() {
    return new AutoValue_HadoopOutputFormatIO_Write.Builder<K, V>().build();
  }

  /**
   * A {@link PTransform} that writes to any data sink which implements Hadoop OutputFormat. For
   * e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres, etc. See the class-level Javadoc on
   * {@link HadoopOutputFormatIO} for more information.
   *
   * @param <K> Type of keys to be written.
   * @param <V> Type of values to be written.
   * @see HadoopOutputFormatIO
   */
  @AutoValue
  public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {
    // Returns the Hadoop Configuration which contains specification of sink.
    @Nullable
    public abstract SerializableConfiguration getConfiguration();

    @Nullable
    public abstract TypeDescriptor<?> getOutputFormatClass();

    @Nullable
    public abstract TypeDescriptor<?> getOutputFormatKeyClass();

    @Nullable
    public abstract TypeDescriptor<?> getOutputFormatValueClass();

    abstract Builder<K, V> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setConfiguration(SerializableConfiguration configuration);

      abstract Builder<K, V> setOutputFormatClass(TypeDescriptor<?> outputFormatClass);

      abstract Builder<K, V> setOutputFormatKeyClass(TypeDescriptor<?> outputFormatKeyClass);

      abstract Builder<K, V> setOutputFormatValueClass(TypeDescriptor<?> outputFormatValueClass);

      abstract Write<K, V> build();
    }

    /** Write to the sink using the options provided by the given configuration. */
    @SuppressWarnings("unchecked")
    public Write<K, V> withConfiguration(Configuration configuration) {
      validateConfiguration(configuration);
      TypeDescriptor<?> outputFormatClass =
          TypeDescriptor.of(configuration.getClass(OUTPUTFORMAT_CLASS, null));
      TypeDescriptor<?> outputFormatKeyClass =
          TypeDescriptor.of(configuration.getClass(OUTPUTFORMAT_KEY_CLASS, null));
      TypeDescriptor<?> outputFormatValueClass =
          TypeDescriptor.of(configuration.getClass(OUTPUTFORMAT_VALUE_CLASS, null));
      Builder<K, V> builder =
          toBuilder().setConfiguration(new SerializableConfiguration(configuration));
      builder.setOutputFormatClass(outputFormatClass);
      builder.setOutputFormatKeyClass(outputFormatKeyClass);
      builder.setOutputFormatValueClass(outputFormatValueClass);

      return builder.build();
    }

    /**
     * Validates that the mandatory configuration properties such as OutputFormat class,
     * OutputFormat key and value classes are provided in the Hadoop configuration.
     */
    private void validateConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "Configuration can not be null");
      checkArgument(
          configuration.get(OUTPUTFORMAT_CLASS) != null,
          "Configuration must contain \"" + OUTPUTFORMAT_CLASS + "\"");
      checkArgument(
          configuration.get(OUTPUTFORMAT_KEY_CLASS) != null,
          "Configuration must contain \"" + OUTPUTFORMAT_KEY_CLASS + "\"");
      checkArgument(
          configuration.get(OUTPUTFORMAT_VALUE_CLASS) != null,
          "Configuration must contain \"" + OUTPUTFORMAT_VALUE_CLASS + "\"");
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {}

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Configuration hadoopConfig = getConfiguration().get();
      if (hadoopConfig != null) {
        builder.addIfNotNull(
            DisplayData.item(OUTPUTFORMAT_CLASS, hadoopConfig.get(OUTPUTFORMAT_CLASS))
                .withLabel("OutputFormat Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUTFORMAT_KEY_CLASS, hadoopConfig.get(OUTPUTFORMAT_KEY_CLASS))
                .withLabel("OutputFormat Key Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUTFORMAT_VALUE_CLASS, hadoopConfig.get(OUTPUTFORMAT_VALUE_CLASS))
                .withLabel("OutputFormat Value Class"));
      }
    }

    @Override
    public PDone expand(PCollection<KV<K, V>> input) {
      input.apply(ParDo.of(new WriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }
  }

  private static class WriteFn<K, V> extends DoFn<KV<K, V>, Void> {
    private final Write<K, V> spec;
    private final SerializableConfiguration conf;
    private transient RecordWriter<K, V> recordWriter;
    private transient OutputCommitter outputCommitter;
    private transient OutputFormat<?, ?> outputFormatObj;
    private transient TaskAttemptContext taskAttemptContext;

    WriteFn(Write<K, V> spec) {
      this.spec = spec;
      conf = spec.getConfiguration();
    }

    @Setup
    public void setup() throws IOException {
      if (recordWriter == null) {

        taskAttemptContext = new TaskAttemptContextImpl(conf.get(), new TaskAttemptID());

        try {
          outputFormatObj =
              (OutputFormat<?, ?>)
                  conf.get()
                      .getClassByName(conf.get().get(OUTPUTFORMAT_CLASS))
                      .getConstructor()
                      .newInstance();
        } catch (InstantiationException
            | IllegalAccessException
            | ClassNotFoundException
            | NoSuchMethodException
            | InvocationTargetException e) {
          throw new IOException("Unable to create OutputFormat object: ", e);
        }

        try {
          LOG.info("Creating new OutputCommitter.");
          outputCommitter = outputFormatObj.getOutputCommitter(taskAttemptContext);
          if (outputCommitter != null) {
            outputCommitter.setupJob(new JobContextImpl(conf.get(), new JobID()));
          } else {
            LOG.warn("OutputCommitter is null.");
          }
        } catch (Exception e) {
          throw new IOException("Unable to create OutputCommitter object: ", e);
        }

        try {
          LOG.info("Creating new RecordWriter.");
          recordWriter = (RecordWriter<K, V>) outputFormatObj.getRecordWriter(taskAttemptContext);
        } catch (InterruptedException e) {
          throw new IOException("Unable to create RecordWriter object: ", e);
        }
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c)
        throws ExecutionException, InterruptedException, IOException {
      recordWriter.write(c.element().getKey(), c.element().getValue());
    }

    @Teardown
    public void teardown() throws Exception {
      if (recordWriter != null) {
        LOG.info("Closing RecordWriter.");
        recordWriter.close(taskAttemptContext);
        recordWriter = null;
      }

      if (outputCommitter != null && outputCommitter.needsTaskCommit(taskAttemptContext)) {
        LOG.info("Commit task for id " + taskAttemptContext.getTaskAttemptID().getTaskID());
        outputCommitter.commitTask(taskAttemptContext);
      }
    }
  }
}
