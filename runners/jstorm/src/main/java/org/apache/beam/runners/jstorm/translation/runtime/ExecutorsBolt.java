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
package org.apache.beam.runners.jstorm.translation.runtime;

import static com.google.common.base.Preconditions.checkNotNull;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.cache.IKvStoreManager;
import com.alibaba.jstorm.cache.KvStoreManagerFactory;
import com.alibaba.jstorm.cluster.Common;
import com.alibaba.jstorm.utils.KryoSerializer;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.jstorm.translation.util.CommonInstance;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExecutorsBolt is a JStorm Bolt composited with several executors chained in a sub-DAG.
 */
public class ExecutorsBolt extends AdaptorBasicBolt {
  private static final long serialVersionUID = -7751043327801735211L;

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorsBolt.class);

  protected ExecutorContext executorContext;

  protected TimerService timerService;

  // map from input tag to executor inside bolt
  protected final Map<TupleTag, Executor> inputTagToExecutor = Maps.newHashMap();
  // set of all output tags that will be emit outside bolt
  protected final Set<TupleTag> outputTags = Sets.newHashSet();
  protected final Set<TupleTag> externalOutputTags = Sets.newHashSet();
  protected final Set<DoFnExecutor> doFnExecutors = Sets.newHashSet();
  protected int internalDoFnExecutorId = 1;
  protected final Map<Integer, DoFnExecutor> idToDoFnExecutor = Maps.newHashMap();

  protected OutputCollector collector;

  protected boolean isStatefulBolt = false;

  protected KryoSerializer<WindowedValue> serializer;

  public ExecutorsBolt() {

  }

  public void setStatefulBolt(boolean isStateful) {
    isStatefulBolt = isStateful;
  }

  public void addExecutor(TupleTag inputTag, Executor executor) {
    inputTagToExecutor.put(
        checkNotNull(inputTag, "inputTag"),
        checkNotNull(executor, "executor"));
  }

  public Map<TupleTag, Executor> getExecutors() {
    return inputTagToExecutor;
  }

  public void registerExecutor(Executor executor) {
    if (executor instanceof DoFnExecutor) {
      DoFnExecutor doFnExecutor = (DoFnExecutor) executor;
      idToDoFnExecutor.put(internalDoFnExecutorId, doFnExecutor);
      doFnExecutor.setInternalDoFnExecutorId(internalDoFnExecutorId);
      internalDoFnExecutorId++;
    }
  }

  public Map<Integer, DoFnExecutor> getIdToDoFnExecutor() {
    return idToDoFnExecutor;
  }

  public void addOutputTags(TupleTag tag) {
    outputTags.add(tag);
  }

  public void addExternalOutputTag(TupleTag<?> tag) {
    externalOutputTags.add(tag);
  }

  public Set<TupleTag> getOutputTags() {
    return outputTags;
  }

  public ExecutorContext getExecutorContext() {
    return executorContext;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    LOG.info("Start to prepare for task-{}", context.getThisTaskId());
    try {
      this.collector = collector;

      // init kv store manager
      String storeName = String.format("task-%d", context.getThisTaskId());
      String stateStorePath = String.format("%s/beam/%s", context.getWorkerIdDir(), storeName);
      IKvStoreManager kvStoreManager = isStatefulBolt
              ? KvStoreManagerFactory.getKvStoreManagerWithMonitor(
              context, storeName, stateStorePath, isStatefulBolt)
              : KvStoreManagerFactory.getKvStoreManager(
              stormConf, storeName, stateStorePath, isStatefulBolt);
      this.executorContext = ExecutorContext.of(context, this, kvStoreManager);

      // init time service
      timerService = initTimerService();

      // init all internal executors
      for (Executor executor : Sets.newHashSet(inputTagToExecutor.values())) {
        executor.init(executorContext);
        if (executor instanceof DoFnExecutor) {
          doFnExecutors.add((DoFnExecutor) executor);
        }
      }

      this.serializer = new KryoSerializer<WindowedValue>(stormConf);

      LOG.info("ExecutorsBolt finished init. LocalExecutors={}", inputTagToExecutor.values());
      LOG.info("inputTagToExecutor={}", inputTagToExecutor);
      LOG.info("outputTags={}", outputTags);
      LOG.info("externalOutputTags={}", externalOutputTags);
      LOG.info("doFnExecutors={}", doFnExecutors);
    } catch (IOException e) {
      throw new RuntimeException("Failed to prepare executors bolt", e);
    }
  }

  public TimerService initTimerService() {
    TopologyContext context = executorContext.getTopologyContext();
    List<Integer> tasks = FluentIterable.from(context.getThisSourceComponentTasks().entrySet())
        .transformAndConcat(
            new Function<Map.Entry<String, List<Integer>>, Iterable<Integer>>() {
              @Override
              public Iterable<Integer> apply(Map.Entry<String, List<Integer>> value) {
                if (Common.isSystemComponent(value.getKey())) {
                  return Collections.EMPTY_LIST;
                } else {
                  return value.getValue();
                }
              }
            })
        .toList();
    TimerService ret = new TimerServiceImpl(executorContext);
    ret.init(tasks);
    return ret;
  }

  @Override
  public void execute(Tuple input) {
    // process a batch
    String streamId = input.getSourceStreamId();
    ITupleExt tuple = (ITupleExt) input;
    Iterator<List<Object>> valueIterator = tuple.batchValues().iterator();
    if (CommonInstance.BEAM_WATERMARK_STREAM_ID.equals(streamId)) {
      while (valueIterator.hasNext()) {
        processWatermark((Long) valueIterator.next().get(0), input.getSourceTask());
      }
    } else {
      doFnStartBundle();
      while (valueIterator.hasNext()) {
        processElement(valueIterator.next(), streamId);
      }
      doFnFinishBundle();
    }
  }

  private void processWatermark(long watermarkTs, int sourceTask) {
    long newWaterMark = timerService.updateInputWatermark(sourceTask, watermarkTs);
    LOG.debug("Recv waterMark-{} from task-{}, newWaterMark={}",
        (new Instant(watermarkTs)).toDateTime(),
        sourceTask,
        (new Instant(newWaterMark)).toDateTime());
    if (newWaterMark != 0) {
      // Some buffer windows are going to be triggered.
      doFnStartBundle();
      timerService.fireTimers(newWaterMark);

      // SideInput: If receiving water mark with max timestamp, It means no more data is supposed
      // to be received from now on. So we are going to process all push back data.
      if (newWaterMark == BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
        for (DoFnExecutor doFnExecutor : doFnExecutors) {
          doFnExecutor.processAllPushBackElements();
        }
      }

      doFnFinishBundle();
    }

    long currentWaterMark = timerService.currentOutputWatermark();
    if (!externalOutputTags.isEmpty()) {
      collector.flush();
      collector.emit(
          CommonInstance.BEAM_WATERMARK_STREAM_ID,
          new Values(currentWaterMark));
      LOG.debug("Send waterMark-{}", (new Instant(currentWaterMark)).toDateTime());
    }
  }

  private void processElement(List<Object> values, String streamId) {
    TupleTag inputTag = new TupleTag(streamId);
    WindowedValue windowedValue = retrieveWindowedValueFromTupleValue(values);
    processExecutorElem(inputTag, windowedValue);
  }

  public <T> void processExecutorElem(TupleTag<T> inputTag, WindowedValue<T> elem) {
    LOG.debug("ProcessExecutorElem: inputTag={}, value={}", inputTag, elem.getValue());
    if (elem != null) {
      Executor executor = inputTagToExecutor.get(inputTag);
      if (executor != null) {
        executor.process(inputTag, elem);
      }
      if (externalOutputTags.contains(inputTag)) {
        emitOutsideBolt(inputTag, elem);
      }
    } else {
      LOG.info("Received null elem for tag={}", inputTag);
    }
  }

  @Override
  public void cleanup() {
    for (Executor executor : Sets.newHashSet(inputTagToExecutor.values())) {
      executor.cleanup();
    }
    executorContext.getKvStoreManager().close();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  public TimerService timerService() {
    return timerService;
  }

  public void setTimerService(TimerService service) {
    timerService = service;
  }

  private WindowedValue retrieveWindowedValueFromTupleValue(List<Object> values) {
    WindowedValue wv = null;
    if (values.size() > 1) {
      Object key = values.get(0);
      WindowedValue value = serializer.deserialize((byte[]) values.get(1));
      wv = value.withValue(KV.of(key, value.getValue()));
    } else {
      wv = serializer.deserialize((byte[]) values.get(0));
    }
    return wv;
  }

  protected void emitOutsideBolt(TupleTag outputTag, WindowedValue outputValue) {
    LOG.debug("Output outside: tag={}, value={}", outputTag, outputValue.getValue());
    if (keyedEmit(outputTag.getId())) {
      KV kv = (KV) outputValue.getValue();
      byte[] immutableOutputValue = serializer.serialize(outputValue.withValue(kv.getValue()));
      // Convert WindowedValue<KV> to <K, WindowedValue<V>>
      if (kv.getKey() == null) {
        // If key is null, emit "null" string here. Because, null value will be ignored in JStorm.
        collector.emit(outputTag.getId(), new Values("null", immutableOutputValue));
      } else {
        collector.emit(outputTag.getId(), new Values(kv.getKey(), immutableOutputValue));
      }
    } else {
      byte[] immutableOutputValue = serializer.serialize(outputValue);
      collector.emit(outputTag.getId(), new Values(immutableOutputValue));
    }
  }

  private void doFnStartBundle() {
    for (DoFnExecutor doFnExecutor : doFnExecutors) {
      doFnExecutor.startBundle();
    }
  }

  private void doFnFinishBundle() {
    for (DoFnExecutor doFnExecutor : doFnExecutors) {
      doFnExecutor.finishBundle();
    }
  }

  @Override
  public String toString() {
    // LOG.info("bolt: " + executorContext.getTopologyContext().toJSONString());
    List<String> ret = new ArrayList<>();
        /*ret.add("inputTags");
        for (TupleTag inputTag : inputTagToExecutor.keySet()) {
            ret.add(inputTag.getId());
        }*/
    ret.add("internalExecutors");
    for (Executor executor : inputTagToExecutor.values()) {
      ret.add(executor.toString());
    }
    ret.add("externalOutputTags");
    for (TupleTag output : externalOutputTags) {
      ret.add(output.getId());
    }
    return Joiner.on('\n').join(ret).concat("\n");
  }
}
