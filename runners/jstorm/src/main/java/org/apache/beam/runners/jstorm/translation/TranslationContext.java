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
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.jstorm.JStormPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state necessary during Pipeline translation to build a Storm topology.
 */
public class TranslationContext {
  private static final Logger LOG = LoggerFactory.getLogger(TranslationContext.class);

  private final UserGraphContext userGraphContext;
  private final ExecutionGraphContext executionGraphContext;

  public TranslationContext(JStormPipelineOptions options) {
    this.userGraphContext = new UserGraphContext(options);
    this.executionGraphContext = new ExecutionGraphContext();
  }

  public ExecutionGraphContext getExecutionGraphContext() {
    return executionGraphContext;
  }

  public UserGraphContext getUserGraphContext() {
    return userGraphContext;
  }

  private void addStormStreamDef(
      TaggedPValue input, String destComponentName, Stream.Grouping grouping) {
    Stream.Producer producer = executionGraphContext.getProducer(input.getValue());
    if (!producer.getComponentId().equals(destComponentName)) {
      Stream.Consumer consumer = Stream.Consumer.of(destComponentName, grouping);
      executionGraphContext.registerStreamConsumer(consumer, producer);

      ExecutorsBolt executorsBolt = executionGraphContext.getBolt(producer.getComponentId());
      if (executorsBolt != null) {
        executorsBolt.addExternalOutputTag(input.getTag());
      }
    }
  }

  private String getUpstreamExecutorsBolt() {
    for (PValue value : userGraphContext.getInputs().values()) {
      String componentId = executionGraphContext.getProducerComponentId(value);
      if (componentId != null && executionGraphContext.getBolt(componentId) != null) {
        return componentId;
      }
    }
    // When upstream component is spout, "null" will be return.
    return null;
  }

  /**
   * check if the current transform is applied to source collection.
   *
   * @return
   */
  private boolean connectedToSource() {
    for (PValue value : userGraphContext.getInputs().values()) {
      if (executionGraphContext.producedBySpout(value)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param upstreamExecutorsBolt
   * @return true if there is multiple input streams, or upstream executor output the same stream
   * to different executors
   */
  private boolean isMultipleInputOrOutput(
      ExecutorsBolt upstreamExecutorsBolt, Map<TupleTag<?>, PValue> inputs) {
    if (inputs.size() > 1) {
      return true;
    } else {
      final Sets.SetView<TupleTag> intersection =
          Sets.intersection(upstreamExecutorsBolt.getExecutors().keySet(), inputs.keySet());
      if (!intersection.isEmpty()) {
        // there is already a different executor consume the same input
        return true;
      } else {
        return false;
      }
    }
  }

  public void addTransformExecutor(Executor executor) {
    addTransformExecutor(executor, Collections.EMPTY_LIST);
  }

  public void addTransformExecutor(Executor executor, List<PValue> sideInputs) {
    addTransformExecutor(
        executor, userGraphContext.getInputs(), userGraphContext.getOutputs(), sideInputs);
  }

  public void addTransformExecutor(
      Executor executor, Map<TupleTag<?>, PValue> inputs, Map<TupleTag<?>, PValue> outputs) {
    addTransformExecutor(executor, inputs, outputs, Collections.EMPTY_LIST);
  }

  public void addTransformExecutor(
      Executor executor,
      Map<TupleTag<?>, PValue> inputs,
      Map<TupleTag<?>, PValue> outputs,
      List<PValue> sideInputs) {
    String name = null;

    ExecutorsBolt bolt = null;

    boolean isGBK = false;
    /**
     * Check if the transform executor needs to be chained into an existing ExecutorsBolt.
     * For following cases, a new bolt is created for the specified executor, otherwise the executor
     * will be added into the bolt contains corresponding upstream executor.
     * a) it is a GroupByKey executor
     * b) it is connected to source directly
     * c) None existing upstream bolt was found
     * d) For the purpose of performance to reduce the side effects between multiple streams which
     *    is output to same executor, a new bolt will be created.
     */
    if (RunnerUtils.isGroupByKeyExecutor(executor)) {
      bolt = new ExecutorsBolt();
      name = executionGraphContext.registerBolt(bolt);
      isGBK = true;
    } else if (connectedToSource()) {
      bolt = new ExecutorsBolt();
      name = executionGraphContext.registerBolt(bolt);
    } else {
      name = getUpstreamExecutorsBolt();
      if (name == null) {
        bolt = new ExecutorsBolt();
        name = executionGraphContext.registerBolt(bolt);
      } else {
        bolt = executionGraphContext.getBolt(name);
        if (isMultipleInputOrOutput(bolt, inputs)) {
          bolt = new ExecutorsBolt();
          name = executionGraphContext.registerBolt(bolt);
        }
      }
    }

    // update the output tags of current transform into ExecutorsBolt
    for (Map.Entry<TupleTag<?>, PValue> entry : outputs.entrySet()) {
      TupleTag tag = entry.getKey();
      PValue value = entry.getValue();

      // use tag of PValueBase
      if (value instanceof PValueBase) {
        tag = ((PValueBase) value).expand().keySet().iterator().next();
      }
      executionGraphContext.registerStreamProducer(
          TaggedPValue.of(tag, value),
          Stream.Producer.of(name, tag.getId(), value.getName()));
      //bolt.addOutputTags(tag);
    }

    // add the transform executor into the chain of ExecutorsBolt
    for (Map.Entry<TupleTag<?>, PValue> entry : inputs.entrySet()) {
      TupleTag tag = entry.getKey();
      PValue value = entry.getValue();
      bolt.addExecutor(tag, executor);

      // filter all connections inside bolt
      //if (!bolt.getOutputTags().contains(tag)) {
      Stream.Grouping grouping;
      if (isGBK) {
        grouping = Stream.Grouping.byFields(Arrays.asList(CommonInstance.KEY));
      } else {
        grouping = Stream.Grouping.of(Stream.Grouping.Type.LOCAL_OR_SHUFFLE);
      }
      addStormStreamDef(TaggedPValue.of(tag, value), name, grouping);
      //}
    }

    for (PValue sideInput : sideInputs) {
      TupleTag tag = userGraphContext.findTupleTag(sideInput);
      bolt.addExecutor(tag, executor);
      checkState(!bolt.getOutputTags().contains(tag));
      addStormStreamDef(
          TaggedPValue.of(tag, sideInput), name, Stream.Grouping.of(Stream.Grouping.Type.ALL));
    }

    bolt.registerExecutor(executor);

    // set parallelismNumber
    String pTransformfullName = userGraphContext.currentTransform.getFullName();
    String compositeName = pTransformfullName.split("/")[0];
    Map parallelismNumMap = userGraphContext.getOptions().getParallelismNumMap();
    if (parallelismNumMap.containsKey(compositeName)) {
      int configNum = (Integer) parallelismNumMap.get(compositeName);
      int currNum = bolt.getParallelismNum();
      bolt.setParallelismNum(Math.max(configNum, currNum));
    }
  }

  /**
   * Context of user graph.
   */
  public static class UserGraphContext {
    private final JStormPipelineOptions options;
    private final Map<PValue, TupleTag> pValueToTupleTag;
    private AppliedPTransform<?, ?, ?> currentTransform = null;

    private boolean isWindowed = false;

    public UserGraphContext(JStormPipelineOptions options) {
      this.options = checkNotNull(options, "options");
      this.pValueToTupleTag = Maps.newHashMap();
    }

    public JStormPipelineOptions getOptions() {
      return this.options;
    }

    public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
      this.currentTransform = transform;
    }

    public String getStepName() {
      return currentTransform.getFullName();
    }

    public <T extends PValue> T getInput() {
      return (T) currentTransform.getInputs().values().iterator().next();
    }

    public Map<TupleTag<?>, PValue> getInputs() {
      return currentTransform.getInputs();
    }

    public TupleTag<?> getInputTag() {
      return currentTransform.getInputs().keySet().iterator().next();
    }

    public List<TupleTag<?>> getInputTags() {
      return Lists.newArrayList(currentTransform.getInputs().keySet());
    }

    public <T extends PValue> T getOutput() {
      return (T) currentTransform.getOutputs().values().iterator().next();
    }

    public Map<TupleTag<?>, PValue> getOutputs() {
      return currentTransform.getOutputs();
    }

    public TupleTag<?> getOutputTag() {
      return currentTransform.getOutputs().keySet().iterator().next();
    }

    public List<TupleTag<?>> getOutputTags() {
      return Lists.newArrayList(currentTransform.getOutputs().keySet());
    }

    public void recordOutputTaggedPValue() {
      for (Map.Entry<TupleTag<?>, PValue> entry : getOutputs().entrySet()) {
        pValueToTupleTag.put(entry.getValue(), entry.getKey());
      }
    }

    public <T> TupleTag<T> findTupleTag(PValue pValue) {
      return pValueToTupleTag.get(checkNotNull(pValue, "pValue"));
    }

    public void setWindowed() {
      this.isWindowed = true;
    }

    public boolean isWindowed() {
      return this.isWindowed;
    }

    @Override
    public String toString() {
      return Joiner.on('\n').join(FluentIterable.from(pValueToTupleTag.entrySet())
          .transform(new Function<Map.Entry<PValue, TupleTag>, String>() {
            @Override
            public String apply(Map.Entry<PValue, TupleTag> entry) {
              return String.format("%s == %s", entry.getValue().getId(), entry.getKey().getName());
            }
          }));
    }
  }

  /**
   * Context of execution graph.
   */
  public static class ExecutionGraphContext {

    private final Map<String, UnboundedSourceSpout> spoutMap = new HashMap<>();
    private final Map<String, ExecutorsBolt> boltMap = new HashMap<>();

    // One-to-one mapping between Stream.Producer and TaggedPValue (or PValue).
    private final Map<PValue, Stream.Producer> pValueToProducer = new HashMap<>();
    private final Map<Stream.Producer, TaggedPValue> producerToTaggedPValue = new HashMap<>();

    private final List<Stream> streams = new ArrayList<>();

    private int id = 1;

    public void registerSpout(UnboundedSourceSpout spout, TaggedPValue output) {
      checkNotNull(spout, "spout");
      checkNotNull(output, "output");
      String name = "spout" + genId();
      this.spoutMap.put(name, spout);
      registerStreamProducer(
          output,
          Stream.Producer.of(name, output.getTag().getId(), output.getValue().getName()));
    }

    public UnboundedSourceSpout getSpout(String id) {
      if (Strings.isNullOrEmpty(id)) {
        return null;
      }
      return this.spoutMap.get(id);
    }

    public Map<String, UnboundedSourceSpout> getSpouts() {
      return this.spoutMap;
    }

    public String registerBolt(ExecutorsBolt bolt) {
      checkNotNull(bolt, "bolt");
      String name = "bolt" + genId();
      this.boltMap.put(name, bolt);
      return name;
    }

    public ExecutorsBolt getBolt(String id) {
      if (Strings.isNullOrEmpty(id)) {
        return null;
      }
      return this.boltMap.get(id);
    }

    public void registerStreamProducer(TaggedPValue taggedPValue, Stream.Producer producer) {
      checkNotNull(taggedPValue, "taggedPValue");
      checkNotNull(producer, "producer");
      pValueToProducer.put(taggedPValue.getValue(), producer);
      producerToTaggedPValue.put(producer, taggedPValue);
    }

    public Stream.Producer getProducer(PValue pValue) {
      return pValueToProducer.get(checkNotNull(pValue, "pValue"));
    }

    public String getProducerComponentId(PValue pValue) {
      Stream.Producer producer = getProducer(pValue);
      return producer == null ? null : producer.getComponentId();
    }

    public boolean producedBySpout(PValue pValue) {
      String componentId = getProducerComponentId(pValue);
      return getSpout(componentId) != null;
    }

    public void registerStreamConsumer(Stream.Consumer consumer, Stream.Producer producer) {
      streams.add(Stream.of(
          checkNotNull(producer, "producer"),
          checkNotNull(consumer, "consumer")));
    }

    public Map<PValue, Stream.Producer> getPValueToProducers() {
      return pValueToProducer;
    }

    public Iterable<Stream> getStreams() {
      return streams;
    }

    @Override
    public String toString() {
      List<String> ret = new ArrayList<>();
      ret.add("SPOUT");
      for (Map.Entry<String, UnboundedSourceSpout> entry : spoutMap.entrySet()) {
        ret.add(entry.getKey() + ": " + entry.getValue().toString());
      }
      ret.add("BOLT");
      for (Map.Entry<String, ExecutorsBolt> entry : boltMap.entrySet()) {
        ret.add(entry.getKey() + ": " + entry.getValue().toString());
      }
      ret.add("STREAM");
      for (Stream stream : streams) {
        ret.add(String.format(
            "%s@@%s ---> %s@@%s",
            stream.getProducer().getStreamId(),
            stream.getProducer().getComponentId(),
            stream.getConsumer().getGrouping(),
            stream.getConsumer().getComponentId()));
      }
      return Joiner.on("\n").join(ret);
    }

    private synchronized int genId() {
      return id++;
    }
  }
}
