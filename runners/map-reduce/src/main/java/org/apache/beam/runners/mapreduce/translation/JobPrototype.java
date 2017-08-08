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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Class that translates a {@link Graphs.FusedStep} to a MapReduce job.
 */
public class JobPrototype {

  public static JobPrototype create(
      int stageId, Graphs.FusedStep fusedStep, PipelineOptions options) {
    return new JobPrototype(stageId, fusedStep, options);
  }

  private final int stageId;
  private final Graphs.FusedStep fusedStep;
  private final PipelineOptions options;

  private JobPrototype(int stageId, Graphs.FusedStep fusedStep, PipelineOptions options) {
    this.stageId = stageId;
    this.fusedStep = checkNotNull(fusedStep, "fusedStep");
    this.options = checkNotNull(options, "options");
  }

  public Job build(Class<?> jarClass, Configuration conf) throws IOException {
    Job job = new Job(conf);
    conf = job.getConfiguration();
    job.setJarByClass(jarClass);
    conf.set(
        "io.serializations",
        "org.apache.hadoop.io.serializer.WritableSerialization,"
            + "org.apache.hadoop.io.serializer.JavaSerialization");

    //TODO: config out dir with PipelineOptions.
    conf.set(
        FileOutputFormat.OUTDIR,
        String.format("/tmp/mapreduce/stage-%d", fusedStep.getStageId()));

    // Setup BoundedSources in BeamInputFormat.
    // TODO: support more than one read steps by introducing a composed BeamInputFormat
    // and a partition operation.
    List<Graphs.Step> readSteps = fusedStep.getStartSteps();
    ArrayList<BoundedSource<?>> sources = new ArrayList<>();
    sources.addAll(
        FluentIterable.from(readSteps)
            .transform(new Function<Graphs.Step, BoundedSource<?>>() {
              @Override
              public BoundedSource<?> apply(Graphs.Step step) {
                checkState(step.getOperation() instanceof SourceOperation);
                return ((SourceOperation) step.getOperation()).getSource();
              }})
            .toList());

    conf.set(
        BeamInputFormat.BEAM_SERIALIZED_BOUNDED_SOURCE,
        Base64.encodeBase64String(SerializableUtils.serializeToByteArray(sources)));
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
          reifyStepName + ".out", new TupleTag<>(), reifyValueCoder);
      Graphs.Step reifyStep = Graphs.Step.of(
          reifyStepName,
          new ReifyTimestampAndWindowsParDoOperation(
              options, operation.getWindowingStrategy(), reifyOutputTag));

      Graphs.Step writeStep = Graphs.Step.of(
          groupByKey.getFullName() + "-Write",
          new ShuffleWriteOperation(kvCoder.getKeyCoder(), reifyValueCoder));

      Graphs.Step gabwStep = Graphs.Step.of(
          groupByKey.getFullName() + "-GroupAlsoByWindows",
          new GroupAlsoByWindowsParDoOperation(options, windowingStrategy, kvCoder, gbkOutTag));

      fusedStep.addStep(
          reifyStep, fusedStep.getInputTags(groupByKey), ImmutableList.of(reifyOutputTag));
      fusedStep.addStep(
          writeStep, ImmutableList.of(reifyOutputTag), Collections.<Graphs.Tag>emptyList());
      fusedStep.addStep(
          gabwStep, Collections.<Graphs.Tag>emptyList(), ImmutableList.of(gbkOutTag));
      fusedStep.removeStep(groupByKey);

      // Setup BeamReducer
      Graphs.Step reducerStartStep = gabwStep;
      chainOperations(reducerStartStep, fusedStep);
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
    Graphs.Tag readOutputTag = Iterables.getOnlyElement(fusedStep.getOutputTags(readSteps.get(0)));
    Graphs.Step mapperStartStep = Iterables.getOnlyElement(fusedStep.getConsumers(readOutputTag));
    chainOperations(mapperStartStep, fusedStep);

    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(byte[].class);
    conf.set(
        BeamMapper.BEAM_PAR_DO_OPERATION_MAPPER,
        Base64.encodeBase64String(
            SerializableUtils.serializeToByteArray(mapperStartStep.getOperation())));
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

  private void chainOperations(Graphs.Step current, Graphs.FusedStep fusedStep) {
    Operation<?> operation = current.getOperation();
    List<Graphs.Tag> outputTags = fusedStep.getOutputTags(current);
    for (Graphs.Tag outTag : outputTags) {
      for (Graphs.Step consumer : fusedStep.getConsumers(outTag)) {
        operation.attachConsumer(outTag.getTupleTag(), consumer.getOperation());
      }
    }
    for (Graphs.Tag outTag : outputTags) {
      for (Graphs.Step consumer : fusedStep.getConsumers(outTag)) {
        chainOperations(consumer, fusedStep);
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
