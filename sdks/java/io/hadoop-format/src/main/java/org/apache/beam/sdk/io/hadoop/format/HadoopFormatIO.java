/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.hadoop.format;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopFormatIO} is a Transform for writing data to any sink which implements Hadoop
 * {@link OutputFormat}. For example - Cassandra, Elasticsearch, HBase, Redis, Postgres etc. {@link
 * HadoopFormatIO} has to make several performance trade-offs in connecting to {@link OutputFormat},
 * so if there is another Beam IO Transform specifically for connecting to your data sink of choice,
 * we would recommend using that one, but this IO Transform allows you to connect to many data sinks
 * that do not yet have a Beam IO Transform.
 *
 * <p>You will need to pass a Hadoop {@link Configuration} with parameters specifying how the write
 * will occur. Many properties of the Configuration are optional, and some are required for certain
 * {@link OutputFormat} classes, but the following properties must be set for all OutputFormats:
 *
 * <ul>
 *   <li>{@code mapreduce.job.id}: The identifier of the write job. E.g.: end timestamp of window.
 *   <li>{@code mapreduce.job.outputformat.class}: The {@link OutputFormat} class used to connect to
 *       your data sink of choice.
 *   <li>{@code mapreduce.job.output.key.class}: The key class passed to the {@link OutputFormat} in
 *       {@code mapreduce.job.outputformat.class}.
 *   <li>{@code mapreduce.job.output.value.class}: The value class passed to the {@link
 *       OutputFormat} in {@code mapreduce.job.outputformat.class}.
 *   <li>{@code mapreduce.job.reduces}: Number of reduce tasks. Value is equal to number of write
 *       tasks which will be genarated. This property is not required for {@link
 *       Write.PartitionedWriterBuilder#withoutPartitioning()} write.
 *   <li>{@code mapreduce.job.partitioner.class}: Hadoop partitioner class which will be used for
 *       distributing of records among partitions. This property is not required for {@link
 *       Write.PartitionedWriterBuilder#withoutPartitioning()} write.
 * </ul>
 *
 * <b>Note:</b> All mentioned values have appropriate constants. E.g.: {@link
 * #OUTPUT_FORMAT_CLASS_ATTR}.
 *
 * <p>For example:
 *
 * <pre>{@code
 * Configuration myHadoopConfiguration = new Configuration(false);
 * // Set Hadoop OutputFormat, key and value class in configuration
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.outputformat.class&quot;,
 *    MyDbOutputFormatClass, OutputFormat.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.output.key.class&quot;,
 *    MyDbOutputFormatKeyClass, Object.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.output.value.class&quot;,
 *    MyDbOutputFormatValueClass, Object.class);
 * myHadoopConfiguration.setClass(&quot;mapreduce.job.output.value.class&quot;,
 *    MyPartitionerClass, Object.class);
 * myHadoopConfiguration.setInt(&quot;mapreduce.job.reduces&quot;, 2);
 * }</pre>
 *
 * <p>You will need to set OutputFormat key and value class (i.e. "mapreduce.job.output.key.class"
 * and "mapreduce.job.output.value.class") in Hadoop {@link Configuration} which are equal to {@code
 * KeyT} and {@code ValueT}. If you set different OutputFormat key or value class than
 * OutputFormat's actual key or value class then, it will throw {@link IllegalArgumentException}
 *
 * <h3>Writing using {@link HadoopFormatIO}</h3>
 *
 * <h4>Batch writing</h4>
 *
 * <pre>{@code
 * //Data which will we want to write
 * PCollection<KV<Text, LongWritable>> boundedWordsCount = ...
 *
 * //Hadoop configuration for write
 * //We have partitioned write, so Partitioner and reducers count have to be set - see withPartitioning() javadoc
 * Configuration myHadoopConfiguration = ...
 * //path to directory with locks
 * String locksDirPath = ...;
 *
 * boundedWordsCount.apply(
 *     "writeBatch",
 *     HadoopFormatIO.<Text, LongWritable>write()
 *         .withConfiguration(myHadoopConfiguration)
 *         .withPartitioning()
 *         .withExternalSynchronization(new HDFSSynchronization(locksDirPath)));
 * }</pre>
 *
 * <h4>Stream writing</h4>
 *
 * <pre>{@code
 *    // Data which will we want to write
 *   PCollection<KV<Text, LongWritable>> unboundedWordsCount = ...;
 *   // Transformation which transforms data of one window into one hadoop configuration
 *   PTransform<PCollection<? extends KV<Text, LongWritable>>, PCollectionView<Configuration>>
 *       configTransform = ...;
 *
 *   unboundedWordsCount.apply(
 *       "writeStream",
 *       HadoopFormatIO.<Text, LongWritable>write()
 *           .withConfigurationTransform(configTransform)
 *           .withExternalSynchronization(new HDFSSynchronization(locksDirPath)));
 * }
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HadoopFormatIO {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopFormatIO.class);

  /** {@link MRJobConfig#OUTPUT_FORMAT_CLASS_ATTR}. */
  public static final String OUTPUT_FORMAT_CLASS_ATTR = MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR;

  /** {@link MRJobConfig#OUTPUT_KEY_CLASS}. */
  public static final String OUTPUT_KEY_CLASS = MRJobConfig.OUTPUT_KEY_CLASS;

  /** {@link MRJobConfig#OUTPUT_VALUE_CLASS}. */
  public static final String OUTPUT_VALUE_CLASS = MRJobConfig.OUTPUT_VALUE_CLASS;

  /** {@link MRJobConfig#NUM_REDUCES}. */
  public static final String NUM_REDUCES = MRJobConfig.NUM_REDUCES;

  /** {@link MRJobConfig#PARTITIONER_CLASS_ATTR}. */
  public static final String PARTITIONER_CLASS_ATTR = MRJobConfig.PARTITIONER_CLASS_ATTR;

  /** {@link MRJobConfig#ID}. */
  public static final String JOB_ID = MRJobConfig.ID;

  /** {@link MRJobConfig#MAPREDUCE_JOB_DIR}. */
  public static final String OUTPUT_DIR = FileOutputFormat.OUTDIR;

  /**
   * Creates an {@link Write.Builder} for creation of Write Transformation. Before creation of the
   * transformation, chain of builders must be set.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   * @return Write builder
   */
  public static <KeyT, ValueT> Write.WriteBuilder<KeyT, ValueT> write() {
    return new Write.Builder<>();
  }

  /**
   * Generates tasks for output pairs and groups them by this key.
   *
   * <p>This transformation is used when is configured write with partitioning.
   *
   * @param <KeyT> type of key
   * @param <ValueT> type of value
   */
  private static class GroupDataByPartition<KeyT, ValueT>
      extends PTransform<
          PCollection<KV<KeyT, ValueT>>, PCollection<KV<Integer, KV<KeyT, ValueT>>>> {

    private PCollectionView<Configuration> configView;

    private GroupDataByPartition(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    @Override
    public PCollection<KV<Integer, KV<KeyT, ValueT>>> expand(PCollection<KV<KeyT, ValueT>> input) {
      return input
          .apply(
              "AssignTask",
              ParDo.of(new AssignTaskFn<KeyT, ValueT>(configView)).withSideInputs(configView))
          .setTypeDescriptor(
              TypeDescriptors.kvs(TypeDescriptors.integers(), input.getTypeDescriptor()))
          .apply("GroupByTaskId", GroupByKey.create())
          .apply("FlattenGroupedTasks", ParDo.of(new FlattenGroupedTasks<>()));
    }
  }

  /**
   * Flattens grouped iterable {@link KV} pairs into triplets of TaskID/Key/Value.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   */
  private static class FlattenGroupedTasks<KeyT, ValueT>
      extends DoFn<KV<Integer, Iterable<KV<KeyT, ValueT>>>, KV<Integer, KV<KeyT, ValueT>>> {

    @ProcessElement
    public void processElement(
        @Element KV<Integer, Iterable<KV<KeyT, ValueT>>> input,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> outputReceiver) {
      final Integer key = input.getKey();
      for (KV<KeyT, ValueT> element :
          requireNonNull(input.getValue(), "Iterable can not be null.")) {
        outputReceiver.output(KV.of(key, element));
      }
    }
  }

  /**
   * A {@link PTransform} that writes to any data sink which implements Hadoop OutputFormat. For
   * e.g. Cassandra, Elasticsearch, HBase, Redis, Postgres, etc. See the class-level Javadoc on
   * {@link HadoopFormatIO} for more information.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   * @see HadoopFormatIO
   */
  public static class Write<KeyT, ValueT> extends PTransform<PCollection<KV<KeyT, ValueT>>, PDone> {

    @Nullable private transient Configuration configuration;

    @Nullable
    private PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>>
        configTransform;

    private ExternalSynchronization externalSynchronization;

    private boolean withPartitioning;

    public Write(
        @Nullable Configuration configuration,
        @Nullable
            PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>>
                configTransform,
        ExternalSynchronization externalSynchronization,
        boolean withPartitioning) {
      this.configuration = configuration;
      this.configTransform = configTransform;
      this.externalSynchronization = externalSynchronization;
      this.withPartitioning = withPartitioning;
    }

    /**
     * Builder for partitioning determining.
     *
     * @param <KeyT> Key type to write
     * @param <ValueT> Value type to write
     */
    public interface PartitionedWriterBuilder<KeyT, ValueT> {

      /**
       * Writes to the sink with partitioning by Task Id.
       *
       * <p>Following Hadoop configuration properties are required with this option:
       *
       * <ul>
       *   <li>{@code mapreduce.job.reduces}: Number of reduce tasks. Value is equal to number of
       *       write tasks which will be genarated.
       *   <li>{@code mapreduce.job.partitioner.class}: Hadoop partitioner class which will be used
       *       for distributing of records among partitions.
       * </ul>
       *
       * @return WriteBuilder for write transformation
       */
      ExternalSynchronizationBuilder<KeyT, ValueT> withPartitioning();

      /**
       * Writes to the sink without need to partition output into specified number of partitions.
       *
       * <p>This write operation doesn't do shuffle by the partition so it saves transfer time
       * before write operation itself. As a consequence it generates random number of partitions.
       *
       * <p><b>Note:</b> Works only for {@link
       * org.apache.beam.sdk.values.PCollection.IsBounded#BOUNDED} {@link PCollection} with global
       * {@link WindowingStrategy}.
       *
       * @return WriteBuilder for write transformation
       */
      ExternalSynchronizationBuilder withoutPartitioning();
    }

    /**
     * Builder for External Synchronization defining.
     *
     * @param <KeyT> Key type to write
     * @param <ValueT> Value type to write
     */
    public interface ExternalSynchronizationBuilder<KeyT, ValueT> {

      /**
       * Specifies class which will provide external synchronization required for hadoop write
       * operation.
       *
       * @param externalSynchronization provider of external synchronization
       * @return Write transformation
       */
      Write<KeyT, ValueT> withExternalSynchronization(
          ExternalSynchronization externalSynchronization);
    }

    /**
     * Main builder of Write transformation.
     *
     * @param <KeyT> Key type to write
     * @param <ValueT> Value type to write
     */
    public interface WriteBuilder<KeyT, ValueT> {

      /**
       * Writes to the sink using the options provided by the given hadoop configuration.
       *
       * <p><b>Note:</b> Works only for {@link
       * org.apache.beam.sdk.values.PCollection.IsBounded#BOUNDED} {@link PCollection} with global
       * {@link WindowingStrategy}.
       *
       * @param config hadoop configuration.
       * @return WriteBuilder with set configuration
       * @throws NullPointerException when the configuration is null
       * @see HadoopFormatIO for required hadoop {@link Configuration} properties
       */
      PartitionedWriterBuilder withConfiguration(Configuration config);

      /**
       * Writes to the sink using configuration created by provided {@code
       * configurationTransformation}.
       *
       * <p>This type is useful especially for processing unbounded windowed data but can be used
       * also for batch processing.
       *
       * <p>Supports only {@link PCollection} with {@link DefaultTrigger}ing and without allowed
       * lateness
       *
       * @param configTransform configuration transformation interface
       * @return WriteBuilder with set configuration transformation
       * @throws NullPointerException when {@code configurationTransformation} is {@code null}
       * @see HadoopFormatIO for required hadoop {@link Configuration} properties
       */
      ExternalSynchronizationBuilder withConfigurationTransform(
          PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>>
              configTransform);
    }

    /**
     * Implementation of all builders.
     *
     * @param <KeyT> Key type to write
     * @param <ValueT> Value type to write
     */
    static class Builder<KeyT, ValueT>
        implements WriteBuilder<KeyT, ValueT>,
            PartitionedWriterBuilder<KeyT, ValueT>,
            ExternalSynchronizationBuilder<KeyT, ValueT> {

      private Configuration configuration;
      private PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>>
          configTransform;
      private ExternalSynchronization externalSynchronization;
      private boolean isWithPartitioning;

      @Override
      public PartitionedWriterBuilder<KeyT, ValueT> withConfiguration(Configuration config) {
        checkNotNull(config, "Hadoop configuration cannot be null");
        this.configuration = config;
        return this;
      }

      @Override
      public ExternalSynchronizationBuilder<KeyT, ValueT> withConfigurationTransform(
          PTransform<PCollection<? extends KV<KeyT, ValueT>>, PCollectionView<Configuration>>
              configTransform) {
        checkNotNull(configTransform, "Configuration transformation cannot be null");
        this.isWithPartitioning = true;
        this.configTransform = configTransform;
        return this;
      }

      @Override
      public ExternalSynchronizationBuilder<KeyT, ValueT> withPartitioning() {
        this.isWithPartitioning = true;
        return this;
      }

      @Override
      public ExternalSynchronizationBuilder<KeyT, ValueT> withoutPartitioning() {
        this.isWithPartitioning = false;
        return this;
      }

      @Override
      public Write<KeyT, ValueT> withExternalSynchronization(
          ExternalSynchronization externalSynchronization) {
        checkNotNull(externalSynchronization, "External synchronization cannot be null");
        this.externalSynchronization = externalSynchronization;
        return new Write<>(
            this.configuration,
            this.configTransform,
            this.externalSynchronization,
            this.isWithPartitioning);
      }
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {}

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Configuration hadoopConfig = configuration;
      if (hadoopConfig != null) {
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_FORMAT_CLASS_ATTR, hadoopConfig.get(OUTPUT_FORMAT_CLASS_ATTR))
                .withLabel("OutputFormat Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_KEY_CLASS, hadoopConfig.get(OUTPUT_KEY_CLASS))
                .withLabel("OutputFormat Key Class"));
        builder.addIfNotNull(
            DisplayData.item(OUTPUT_VALUE_CLASS, hadoopConfig.get(OUTPUT_VALUE_CLASS))
                .withLabel("OutputFormat Value Class"));
        builder.addIfNotNull(
            DisplayData.item(
                    PARTITIONER_CLASS_ATTR,
                    hadoopConfig.get(
                        PARTITIONER_CLASS_ATTR,
                        HadoopFormats.DEFAULT_PARTITIONER_CLASS_ATTR.getName()))
                .withLabel("Partitioner Class"));
      }
    }

    @Override
    public PDone expand(PCollection<KV<KeyT, ValueT>> input) {

      // streamed pipeline must have defined configuration transformation
      if (input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)
          || !input.getWindowingStrategy().equals(WindowingStrategy.globalDefault())) {
        checkArgument(
            configTransform != null,
            "Writing of unbounded data can be processed only with configuration transformation provider. See %s.withConfigurationTransform()",
            Write.class);
      }

      verifyInputWindowing(input);

      TypeDescriptor<Configuration> configType = new TypeDescriptor<Configuration>() {};
      input
          .getPipeline()
          .getCoderRegistry()
          .registerCoderForType(configType, new ConfigurationCoder());

      PCollectionView<Configuration> configView = createConfigurationView(input);

      return processJob(input, configView);
    }

    /**
     * Processes write job. Write job is composed from following partial steps:
     *
     * <ul>
     *   <li>When partitioning is enabled:
     *       <ul>
     *         <li>Assigning of the {@link TaskID} (represented as {@link Integer}) to the {@link
     *             KV}s in {@link AssignTaskFn}
     *         <li>Grouping {@link KV}s by the {@link TaskID}
     *       </ul>
     *   <li>Otherwise creation of TaskId via {@link PrepareNonPartitionedTasksFn} where locks are
     *       created for each task id
     *   <li>Writing of {@link KV} records via {@link WriteFn}
     *   <li>Committing of whole job via {@link CommitJobFn}
     * </ul>
     *
     * @param input Collection with output data to write
     * @param configView configuration view
     * @return Successfully processed write
     */
    private PDone processJob(
        PCollection<KV<KeyT, ValueT>> input, PCollectionView<Configuration> configView) {

      TypeDescriptor<Iterable<Integer>> iterableIntType =
          TypeDescriptors.iterables(TypeDescriptors.integers());

      PCollection<KV<KeyT, ValueT>> validatedInput =
          input.apply(
              ParDo.of(
                      new SetupJobFn<>(
                          externalSynchronization, configView, input.getTypeDescriptor()))
                  .withSideInputs(configView));

      PCollection<KV<Integer, KV<KeyT, ValueT>>> writeData =
          withPartitioning
              ? validatedInput.apply("GroupDataByPartition", new GroupDataByPartition<>(configView))
              : validatedInput.apply(
                  "PrepareNonPartitionedTasks",
                  ParDo.of(
                          new PrepareNonPartitionedTasksFn<KeyT, ValueT>(
                              configView, externalSynchronization))
                      .withSideInputs(configView));

      PCollection<Iterable<Integer>> collectedFinishedWrites =
          writeData
              .apply(
                  "Write",
                  ParDo.of(new WriteFn<KeyT, ValueT>(configView, externalSynchronization))
                      .withSideInputs(configView))
              .setTypeDescriptor(TypeDescriptors.integers())
              .apply(
                  "CollectWriteTasks",
                  Combine.globally(new IterableCombinerFn<>(TypeDescriptors.integers()))
                      .withoutDefaults())
              .setTypeDescriptor(iterableIntType);

      return PDone.in(
          collectedFinishedWrites
              .apply(
                  "CommitWriteJob",
                  ParDo.of(new CommitJobFn<Integer>(configView, externalSynchronization))
                      .withSideInputs(configView))
              .getPipeline());
    }

    private void verifyInputWindowing(PCollection<KV<KeyT, ValueT>> input) {
      if (input.isBounded().equals(PCollection.IsBounded.UNBOUNDED)) {
        checkArgument(
            !input.getWindowingStrategy().equals(WindowingStrategy.globalDefault()),
            "Cannot work with %s and GLOBAL %s",
            PCollection.IsBounded.UNBOUNDED,
            WindowingStrategy.class.getSimpleName());
        checkArgument(
            input.getWindowingStrategy().getTrigger().getClass().equals(DefaultTrigger.class),
            "Cannot work with %s trigger. Write works correctly only with %s",
            input.getWindowingStrategy().getTrigger().getClass().getSimpleName(),
            DefaultTrigger.class.getSimpleName());
        checkArgument(
            input.getWindowingStrategy().getAllowedLateness().equals(Duration.ZERO),
            "Write does not allow late data.");
      }
    }

    /**
     * Creates {@link PCollectionView} with one {@link Configuration} based on the set source of the
     * configuration.
     *
     * @param input input data
     * @return PCollectionView with single {@link Configuration}
     * @see Builder#withConfiguration(Configuration)
     * @see Builder#withConfigurationTransform(PTransform)
     */
    private PCollectionView<Configuration> createConfigurationView(
        PCollection<KV<KeyT, ValueT>> input) {

      PCollectionView<Configuration> config;
      if (configuration != null) {
        config =
            input
                .getPipeline()
                .apply("CreateOutputConfig", Create.<Configuration>of(configuration))
                .apply(View.<Configuration>asSingleton().withDefaultValue(configuration));
      } else {
        config = input.apply("TransformDataIntoConfig", configTransform);
      }

      return config;
    }
  }

  /**
   * Represents context of one hadoop write task.
   *
   * @param <KeyT> Key type to write
   * @param <ValueT> Value type to write
   */
  private static class TaskContext<KeyT, ValueT> {

    private RecordWriter<KeyT, ValueT> recordWriter;
    private OutputCommitter outputCommitter;
    private OutputFormat<KeyT, ValueT> outputFormatObj;
    private TaskAttemptContext taskAttemptContext;

    TaskContext(TaskAttemptID taskAttempt, Configuration conf) {
      taskAttemptContext = HadoopFormats.createTaskAttemptContext(conf, taskAttempt);
      outputFormatObj = HadoopFormats.createOutputFormatFromConfig(conf);
      outputCommitter = initOutputCommitter(outputFormatObj, conf, taskAttemptContext);
      recordWriter = initRecordWriter(outputFormatObj, taskAttemptContext);
    }

    RecordWriter<KeyT, ValueT> getRecordWriter() {
      return recordWriter;
    }

    OutputCommitter getOutputCommitter() {
      return outputCommitter;
    }

    TaskAttemptContext getTaskAttemptContext() {
      return taskAttemptContext;
    }

    int getTaskId() {
      return taskAttemptContext.getTaskAttemptID().getTaskID().getId();
    }

    String getJobId() {
      return taskAttemptContext.getJobID().getJtIdentifier();
    }

    void abortTask() {
      try {
        outputCommitter.abortTask(taskAttemptContext);
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("Unable to abort task %s of job %s", getTaskId(), getJobId()));
      }
    }

    private RecordWriter<KeyT, ValueT> initRecordWriter(
        OutputFormat<KeyT, ValueT> outputFormatObj, TaskAttemptContext taskAttemptContext)
        throws IllegalStateException {
      try {
        LOGGER.info(
            "Creating new RecordWriter for task {} of Job with id {}.",
            taskAttemptContext.getTaskAttemptID().getTaskID().getId(),
            taskAttemptContext.getJobID().getJtIdentifier());
        return outputFormatObj.getRecordWriter(taskAttemptContext);
      } catch (InterruptedException | IOException e) {
        throw new IllegalStateException("Unable to create RecordWriter object: ", e);
      }
    }

    private static OutputCommitter initOutputCommitter(
        OutputFormat<?, ?> outputFormatObj,
        Configuration conf,
        TaskAttemptContext taskAttemptContext)
        throws IllegalStateException {
      OutputCommitter outputCommitter;
      try {
        outputCommitter = outputFormatObj.getOutputCommitter(taskAttemptContext);
        if (outputCommitter != null) {
          outputCommitter.setupJob(new JobContextImpl(conf, taskAttemptContext.getJobID()));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Unable to create OutputCommitter object: ", e);
      }

      return outputCommitter;
    }

    @Override
    public String toString() {
      return "TaskContext{"
          + "jobId="
          + getJobId()
          + ", taskId="
          + getTaskId()
          + ", attemptId="
          + taskAttemptContext.getTaskAttemptID().getId()
          + '}';
    }
  }

  /** Coder of configuration instances. */
  private static class ConfigurationCoder extends AtomicCoder<Configuration> {

    @Override
    public void encode(Configuration value, OutputStream outStream) throws IOException {
      DataOutputStream dataOutputStream = new DataOutputStream(outStream);
      value.write(dataOutputStream);
      dataOutputStream.flush();
    }

    @Override
    public Configuration decode(InputStream inStream) throws IOException {
      DataInputStream dataInputStream = new DataInputStream(inStream);
      Configuration config = new Configuration(false);
      config.readFields(dataInputStream);

      return config;
    }
  }

  /**
   * DoFn with following responsibilities:
   *
   * <ul>
   *   <li>Validates configuration - checks whether all required properties are set.
   *   <li>Validates types of input PCollection elements.
   *   <li>Setups start of the {@link OutputFormat} job for given window.
   * </ul>
   *
   * <p>Logic of the setup job is called only for the very first element of the instance. All other
   * elements are directly sent to next processing.
   */
  private static class SetupJobFn<KeyT, ValueT> extends DoFn<KV<KeyT, ValueT>, KV<KeyT, ValueT>> {

    private ExternalSynchronization externalSynchronization;
    private PCollectionView<Configuration> configView;
    private TypeDescriptor<KV<KeyT, ValueT>> inputTypeDescriptor;
    private boolean isSetupJobAttempted;

    SetupJobFn(
        ExternalSynchronization externalSynchronization,
        PCollectionView<Configuration> configView,
        TypeDescriptor<KV<KeyT, ValueT>> inputTypeDescriptor) {
      this.externalSynchronization = externalSynchronization;
      this.configView = configView;
      this.inputTypeDescriptor = inputTypeDescriptor;
    }

    @Setup
    public void setup() {
      isSetupJobAttempted = false;
    }

    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<KeyT, ValueT>> receiver,
        BoundedWindow window,
        ProcessContext c) {

      receiver.output(element);

      if (isSetupJobAttempted) {
        // setup of job was already attempted
        return;
      }

      Configuration conf = c.sideInput(configView);

      // validate configuration and input
      // must be done first, because in all later operations are required assumptions from
      // validation
      validateConfiguration(conf);
      validateInputData(conf);

      boolean isJobLockAcquired = externalSynchronization.tryAcquireJobLock(conf);
      isSetupJobAttempted = true;

      if (!isJobLockAcquired) {
        // some parallel execution acquired task
        return;
      }

      try {
        // setup job
        JobID jobId = HadoopFormats.getJobId(conf);

        trySetupJob(jobId, conf, window);

      } catch (Exception e) {
        //        externalSynchronization.releaseJobIdLock(conf);
        throw new IllegalStateException(e);
      }
    }

    /**
     * Validates that the mandatory configuration properties such as OutputFormat class,
     * OutputFormat key and value classes are provided in the Hadoop configuration.
     */
    private void validateConfiguration(Configuration conf) {

      checkArgument(conf != null, "Configuration can not be null");
      checkArgument(
          conf.get(OUTPUT_FORMAT_CLASS_ATTR) != null,
          "Configuration must contain \"" + OUTPUT_FORMAT_CLASS_ATTR + "\"");
      checkArgument(
          conf.get(OUTPUT_KEY_CLASS) != null,
          "Configuration must contain \"" + OUTPUT_KEY_CLASS + "\"");
      checkArgument(
          conf.get(OUTPUT_VALUE_CLASS) != null,
          "Configuration must contain \"" + OUTPUT_VALUE_CLASS + "\"");
      checkArgument(conf.get(JOB_ID) != null, "Configuration must contain \"" + JOB_ID + "\"");
    }

    /**
     * Validates input data whether have correctly specified {@link TypeDescriptor}s of input data
     * and if the {@link TypeDescriptor}s match with output types set in the hadoop {@link
     * Configuration}.
     *
     * @param conf hadoop config
     */
    @SuppressWarnings("unchecked")
    private void validateInputData(Configuration conf) {
      TypeDescriptor<KeyT> outputFormatKeyClass =
          (TypeDescriptor<KeyT>) TypeDescriptor.of(conf.getClass(OUTPUT_KEY_CLASS, null));
      TypeDescriptor<ValueT> outputFormatValueClass =
          (TypeDescriptor<ValueT>) TypeDescriptor.of(conf.getClass(OUTPUT_VALUE_CLASS, null));

      checkArgument(
          inputTypeDescriptor != null,
          "Input %s must be set!",
          TypeDescriptor.class.getSimpleName());
      checkArgument(
          KV.class.equals(inputTypeDescriptor.getRawType()),
          "%s expects %s as input type.",
          Write.class.getSimpleName(),
          KV.class);
      checkArgument(
          inputTypeDescriptor.equals(
              TypeDescriptors.kvs(outputFormatKeyClass, outputFormatValueClass)),
          "%s expects following %ss: KV(Key: %s, Value: %s) but following %ss are set: KV(Key: %s, Value: %s)",
          Write.class.getSimpleName(),
          TypeDescriptor.class.getSimpleName(),
          outputFormatKeyClass.getRawType(),
          outputFormatValueClass.getRawType(),
          TypeDescriptor.class.getSimpleName(),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[0]),
          inputTypeDescriptor.resolveType(KV.class.getTypeParameters()[1]));
    }

    /**
     * Setups the hadoop write job as running. There is possibility that some parallel worker
     * already did setup. in this case {@link FileAlreadyExistsException} is catch.
     *
     * @param jobId jobId
     * @param conf hadoop configuration
     * @param window window
     */
    private void trySetupJob(JobID jobId, Configuration conf, BoundedWindow window) {
      try {
        TaskAttemptContext setupTaskContext = HadoopFormats.createSetupTaskContext(conf, jobId);
        OutputFormat<?, ?> jobOutputFormat = HadoopFormats.createOutputFormatFromConfig(conf);

        jobOutputFormat.checkOutputSpecs(setupTaskContext);
        jobOutputFormat.getOutputCommitter(setupTaskContext).setupJob(setupTaskContext);

        LOGGER.info(
            "Job with id {} successfully configured from window with max timestamp {}.",
            jobId.getJtIdentifier(),
            window.maxTimestamp());

      } catch (FileAlreadyExistsException e) {
        LOGGER.info("Job was already set by other worker. Skipping rest of the setup.");
      } catch (Exception e) {
        throw new RuntimeException("Unable to setup job.", e);
      }
    }
  }

  /**
   * Commits whole write job.
   *
   * @param <T> type of TaskId identifier
   */
  private static class CommitJobFn<T> extends DoFn<Iterable<T>, Void> {

    private PCollectionView<Configuration> configView;
    private ExternalSynchronization externalSynchronization;

    CommitJobFn(
        PCollectionView<Configuration> configView,
        ExternalSynchronization externalSynchronization) {
      this.configView = configView;
      this.externalSynchronization = externalSynchronization;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {

      Configuration config = c.sideInput(configView);
      cleanupJob(config);
    }

    /**
     * Commits whole write job.
     *
     * @param config hadoop config
     */
    private void cleanupJob(Configuration config) {

      externalSynchronization.releaseJobIdLock(config);

      JobID jobID = HadoopFormats.getJobId(config);
      TaskAttemptContext cleanupTaskContext = HadoopFormats.createCleanupTaskContext(config, jobID);
      OutputFormat<?, ?> outputFormat = HadoopFormats.createOutputFormatFromConfig(config);
      try {
        OutputCommitter outputCommitter = outputFormat.getOutputCommitter(cleanupTaskContext);
        outputCommitter.commitJob(cleanupTaskContext);
      } catch (Exception e) {
        throw new RuntimeException("Unable to commit job.", e);
      }
    }
  }

  /**
   * Assigns {@link TaskID#getId()} to the given pair of key and value. {@link TaskID} is later used
   * for writing the pair to hadoop file.
   *
   * @param <KeyT> Type of key
   * @param <ValueT> Type of value
   */
  private static class AssignTaskFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Integer, KV<KeyT, ValueT>>> {

    private PCollectionView<Configuration> configView;

    // Transient properties because they are used only for one bundle
    /** Cache of created TaskIDs for given bundle. */
    private transient Map<Integer, TaskID> partitionToTaskContext;

    private transient Partitioner<KeyT, ValueT> partitioner;
    private transient Integer reducersCount;
    private transient JobID jobId;

    /**
     * Needs configuration view of given window.
     *
     * @param configView configuration view
     */
    AssignTaskFn(PCollectionView<Configuration> configView) {
      this.configView = configView;
    }

    /** Deletes cached fields used in previous bundle. */
    @StartBundle
    public void startBundle() {
      partitionToTaskContext = new HashMap<>();
      partitioner = null;
      jobId = null;
      reducersCount = null;
    }

    @ProcessElement
    public void processElement(
        @Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> receiver,
        ProcessContext c) {

      Configuration config = c.sideInput(configView);

      TaskID taskID = createTaskIDForKV(element, config);
      int taskId = taskID.getId();
      receiver.output(KV.of(taskId, element));
    }

    /**
     * Creates or reuses existing {@link TaskID} for given record.
     *
     * <p>The {@link TaskID} creation is based on the calculation hash function of {@code KeyT} of
     * the pair via {@link Partitioner} (stored in configuration)
     *
     * @param kv keyvalue pair which should be written
     * @param config hadoop configuration
     * @return TaskID assigned to given record
     */
    private TaskID createTaskIDForKV(KV<KeyT, ValueT> kv, Configuration config) {
      int taskContextKey =
          getPartitioner(config).getPartition(kv.getKey(), kv.getValue(), getReducersCount(config));

      return partitionToTaskContext.computeIfAbsent(
          taskContextKey, (key) -> HadoopFormats.createTaskID(getJobId(config), key));
    }

    private JobID getJobId(Configuration config) {
      if (jobId == null) {
        jobId = HadoopFormats.getJobId(config);
      }
      return jobId;
    }

    private int getReducersCount(Configuration config) {
      if (reducersCount == null) {
        reducersCount = HadoopFormats.getReducersCount(config);
      }
      return reducersCount;
    }

    private Partitioner<KeyT, ValueT> getPartitioner(Configuration config) {
      if (partitioner == null) {
        partitioner = HadoopFormats.getPartitioner(config);
      }
      return partitioner;
    }
  }

  /**
   * Writes all {@link KV}s pair for given {@link TaskID} (Task Id determines partition of writing).
   *
   * <p>For every {@link TaskID} are executed following steps:
   *
   * <ul>
   *   <li>Creation of {@link TaskContext} on start of bundle
   *   <li>Writing every single {@link KV} pair via {@link RecordWriter}.
   *   <li>Committing of task on bundle finish
   * </ul>
   *
   * @param <KeyT> Type of key
   * @param <ValueT> Type of value
   */
  private static class WriteFn<KeyT, ValueT> extends DoFn<KV<Integer, KV<KeyT, ValueT>>, Integer> {

    private final PCollectionView<Configuration> configView;
    private final ExternalSynchronization externalSynchronization;

    // Transient property because they are used only for one bundle
    /** Key by combination of Window and TaskId because different windows can use same TaskId. */
    private transient Map<KV<BoundedWindow, Integer>, TaskContext<KeyT, ValueT>>
        bundleTaskContextMap;

    WriteFn(
        PCollectionView<Configuration> configView,
        ExternalSynchronization externalSynchronization) {
      this.configView = configView;
      this.externalSynchronization = externalSynchronization;
    }

    /** Deletes cached map from previous bundle. */
    @StartBundle
    public void startBundle() {
      bundleTaskContextMap = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
        @Element KV<Integer, KV<KeyT, ValueT>> element, ProcessContext c, BoundedWindow b) {

      Integer taskID = element.getKey();
      KV<BoundedWindow, Integer> win2TaskId = KV.of(b, taskID);

      TaskContext<KeyT, ValueT> taskContext =
          bundleTaskContextMap.computeIfAbsent(win2TaskId, w2t -> setupTask(w2t.getValue(), c));

      write(element.getValue(), taskContext);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      if (bundleTaskContextMap == null) {
        return;
      }

      for (Map.Entry<KV<BoundedWindow, Integer>, TaskContext<KeyT, ValueT>> entry :
          bundleTaskContextMap.entrySet()) {
        TaskContext<KeyT, ValueT> taskContext = entry.getValue();

        try {
          taskContext.getRecordWriter().close(taskContext.getTaskAttemptContext());
          taskContext.getOutputCommitter().commitTask(taskContext.getTaskAttemptContext());

          LOGGER.info("Write task for {} was successfully committed!", taskContext);
        } catch (Exception e) {
          processTaskException(taskContext, e);
        }

        BoundedWindow window = entry.getKey().getKey();
        c.output(taskContext.getTaskId(), Objects.requireNonNull(window).maxTimestamp(), window);
      }
    }

    private void processTaskException(TaskContext<KeyT, ValueT> taskContext, Exception e) {
      LOGGER.warn("Write task for {} failed. Will abort task.", taskContext);
      taskContext.abortTask();
      throw new IllegalArgumentException(e);
    }

    /**
     * Writes one {@link KV} pair for given {@link TaskID}.
     *
     * @param kv Iterable of pairs to write
     * @param taskContext taskContext
     */
    private void write(KV<KeyT, ValueT> kv, TaskContext<KeyT, ValueT> taskContext) {

      try {
        RecordWriter<KeyT, ValueT> recordWriter = taskContext.getRecordWriter();
        recordWriter.write(kv.getKey(), kv.getValue());
      } catch (Exception e) {
        processTaskException(taskContext, e);
      }
    }

    /**
     * Creates {@link TaskContext} and setups write for given {@code taskId}.
     *
     * @param taskId id of the write Task
     * @param c process context
     * @return created TaskContext
     * @throws IllegalStateException if the setup of the write task failed
     */
    private TaskContext<KeyT, ValueT> setupTask(Integer taskId, ProcessContext c)
        throws IllegalStateException {

      final Configuration conf = c.sideInput(configView);
      TaskAttemptID taskAttemptID = externalSynchronization.acquireTaskAttemptIdLock(conf, taskId);

      TaskContext<KeyT, ValueT> taskContext = new TaskContext<>(taskAttemptID, conf);

      try {
        taskContext.getOutputCommitter().setupTask(taskContext.getTaskAttemptContext());
      } catch (Exception e) {
        processTaskException(taskContext, e);
      }

      LOGGER.info(
          "Task with id {} of job {} was successfully setup!",
          taskId,
          HadoopFormats.getJobId(conf).getJtIdentifier());

      return taskContext;
    }
  }

  /**
   * Registers task ID for each bundle without need to group data by taskId. Every bundle reserves
   * its own taskId via particular implementation of {@link ExternalSynchronization} class.
   *
   * @param <KeyT> Type of keys to be written.
   * @param <ValueT> Type of values to be written.
   */
  private static class PrepareNonPartitionedTasksFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<Integer, KV<KeyT, ValueT>>> {

    private transient TaskID taskId;

    private final PCollectionView<Configuration> configView;
    private final ExternalSynchronization externalSynchronization;

    private PrepareNonPartitionedTasksFn(
        PCollectionView<Configuration> configView,
        ExternalSynchronization externalSynchronization) {
      this.configView = configView;
      this.externalSynchronization = externalSynchronization;
    }

    @DoFn.StartBundle
    public void startBundle() {
      taskId = null;
    }

    @DoFn.ProcessElement
    public void processElement(
        @DoFn.Element KV<KeyT, ValueT> element,
        OutputReceiver<KV<Integer, KV<KeyT, ValueT>>> output,
        ProcessContext c) {

      if (taskId == null) {
        Configuration conf = c.sideInput(configView);
        taskId = externalSynchronization.acquireTaskIdLock(conf);
      }

      output.output(KV.of(taskId.getId(), element));
    }
  }
}
