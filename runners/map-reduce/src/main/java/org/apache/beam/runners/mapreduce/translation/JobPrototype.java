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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.mapreduce.MapReducePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Class that translates a {@link Graphs.FusedStep} to a MapReduce job.
 */
public class JobPrototype {

  public static JobPrototype create(
      int stageId, Graphs.FusedStep fusedStep, MapReducePipelineOptions options) {
    return new JobPrototype(stageId, fusedStep, options);
  }

  private final int stageId;
  private final Graphs.FusedStep fusedStep;
  private final MapReducePipelineOptions options;
  private final ConfigurationUtils configUtils;

  private JobPrototype(int stageId, Graphs.FusedStep fusedStep, MapReducePipelineOptions options) {
    this.stageId = stageId;
    this.fusedStep = checkNotNull(fusedStep, "fusedStep");
    this.options = checkNotNull(options, "options");
    this.configUtils = new ConfigurationUtils(options);
  }

  public Job build(Class<?> jarClass, Configuration initConf) throws IOException {
    Job job = new Job(initConf);
    final Configuration conf = job.getConfiguration();
    job.setJarByClass(jarClass);
    conf.set(
        "io.serializations",
        "org.apache.hadoop.io.serializer.WritableSerialization,"
            + "org.apache.hadoop.io.serializer.JavaSerialization");
    conf.set("mapreduce.job.counters.group.name.max", "512");
    Limits.init(conf);

    conf.set(
        FileOutputFormat.OUTDIR,
        configUtils.getFileOutputDir(fusedStep.getStageId()));

    // Setup BoundedSources in BeamInputFormat.
    Graphs.Step startStep = Iterables.getOnlyElement(fusedStep.getStartSteps());
    checkState(startStep.getOperation() instanceof PartitionOperation);
    PartitionOperation partitionOperation = (PartitionOperation) startStep.getOperation();

    ArrayList<ReadOperation.TaggedSource> taggedSources = new ArrayList<>();
    taggedSources.addAll(FluentIterable.from(partitionOperation
        .getReadOperations())
        .transform(new Function<ReadOperation, ReadOperation.TaggedSource>() {
          @Override
          public ReadOperation.TaggedSource apply(ReadOperation operation) {
            return operation.getTaggedSource(conf);
          }})
        .toList());
    conf.set(
        BeamInputFormat.BEAM_SERIALIZED_BOUNDED_SOURCE,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(
            taggedSources)));
    conf.set(
        BeamInputFormat.BEAM_SERIALIZED_PIPELINE_OPTIONS,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(
            new SerializedPipelineOptions(options))));
    job.setInputFormatClass(BeamInputFormat.class);

    if (fusedStep.containsGroupByKey()) {
      Graphs.Step groupByKey = fusedStep.getGroupByKeyStep();
      Graphs.Tag gbkOutTag = Iterables.getOnlyElement(fusedStep.getOutputTags(groupByKey));
      GroupByKeyOperation operation = (GroupByKeyOperation) groupByKey.getOperation();
      WindowingStrategy<?, ?> windowingStrategy = operation.getWindowingStrategy();
      KvCoder<?, ?> kvCoder = operation.getKvCoder();

      String reifyStepName = groupByKey.getFullName() + "-Reify";
      Coder<?> reifyValueCoder = getReifyValueCoder(kvCoder.getValueCoder(), windowingStrategy);
      Graphs.Tag reifyOutputTag = Graphs.Tag.of(
          reifyStepName + ".out", new TupleTag<>(), reifyValueCoder, windowingStrategy);
      Graphs.Step reifyStep = Graphs.Step.of(
          reifyStepName,
          new ReifyTimestampAndWindowsParDoOperation(
              reifyStepName, options, operation.getWindowingStrategy(), reifyOutputTag));

      Graphs.Step writeStep = Graphs.Step.of(
          groupByKey.getFullName() + "-Write",
          new ShuffleWriteOperation(kvCoder.getKeyCoder(), reifyValueCoder));

      String gabwStepName = groupByKey.getFullName() + "-GroupAlsoByWindows";
      Graphs.Step gabwStep = Graphs.Step.of(
          gabwStepName,
          new GroupAlsoByWindowsParDoOperation(
              gabwStepName, options, windowingStrategy, kvCoder, gbkOutTag));

      fusedStep.addStep(
          reifyStep, fusedStep.getInputTags(groupByKey), ImmutableList.of(reifyOutputTag));
      fusedStep.addStep(
          writeStep, ImmutableList.of(reifyOutputTag), Collections.<Graphs.Tag>emptyList());
      fusedStep.addStep(
          gabwStep, Collections.<Graphs.Tag>emptyList(), ImmutableList.of(gbkOutTag));
      fusedStep.removeStep(groupByKey);

      // Setup BeamReducer
      Graphs.Step reducerStartStep = gabwStep;
      chainOperations(reducerStartStep, fusedStep, Sets.<Graphs.Step>newHashSet());
      conf.set(
          BeamReducer.BEAM_REDUCER_KV_CODER,
          Base64.encodeBase64String(SerializableUtils.serializeToByteArray(
              KvCoder.of(kvCoder.getKeyCoder(), reifyValueCoder))));
      conf.set(
          BeamReducer.BEAM_PAR_DO_OPERATION_REDUCER,
          Base64.encodeBase64String(
              SerializableUtils.serializeToByteArray(reducerStartStep.getOperation())));
      job.setReducerClass(BeamReducer.class);
    }

    // Setup DoFns in BeamMapper.
    chainOperations(startStep, fusedStep, Sets.<Graphs.Step>newHashSet());

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(byte[].class);
    conf.set(
        BeamMapper.BEAM_PAR_DO_OPERATION_MAPPER,
        Base64.encodeBase64String(
            SerializableUtils.serializeToByteArray(startStep.getOperation())));
    job.setMapperClass(BeamMapper.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    for (Graphs.Step step : fusedStep.getSteps()) {
      if (step.getOperation() instanceof FileWriteOperation) {
        FileWriteOperation writeOperation = (FileWriteOperation) step.getOperation();
        //SequenceFileOutputFormat.setOutputPath(job, new Path("/tmp/mapreduce/"));
        MultipleOutputs.addNamedOutput(
            job,
            writeOperation.getFileName(),
            SequenceFileOutputFormat.class,
            NullWritable.class, BytesWritable.class);
      }
    }
    return job;
  }

  private void chainOperations(
      Graphs.Step current, Graphs.FusedStep fusedStep, Set<Graphs.Step> visited) {
    Operation<?> operation = current.getOperation();
    List<Graphs.Tag> outputTags = fusedStep.getOutputTags(current);
    for (Graphs.Tag outTag : outputTags) {
      for (Graphs.Step consumer : fusedStep.getConsumers(outTag)) {
        operation.attachConsumer(outTag.getTupleTag(), consumer.getOperation());
      }
    }
    visited.add(current);
    for (Graphs.Tag outTag : outputTags) {
      for (Graphs.Step consumer : fusedStep.getConsumers(outTag)) {
        if (!visited.contains(consumer)) {
          chainOperations(consumer, fusedStep, visited);
        }
      }
    }
  }

  private Coder<Object> getReifyValueCoder(
      Coder<?> valueCoder, WindowingStrategy<?, ?> windowingStrategy) {
    // TODO: do we need full coder to encode windows.
    return (Coder) WindowedValue.getFullCoder(
        valueCoder, windowingStrategy.getWindowFn().windowCoder());
  }
}
