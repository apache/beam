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
package org.apache.beam.runners.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.jstorm.serialization.BeamSdkRepackUtilsSerializer;
import org.apache.beam.runners.jstorm.serialization.BeamUtilsSerializer;
import org.apache.beam.runners.jstorm.serialization.GuavaUtilsSerializer;
import org.apache.beam.runners.jstorm.serialization.JStormUtilsSerializer;
import org.apache.beam.runners.jstorm.serialization.JavaUtilsSerializer;
import org.apache.beam.runners.jstorm.translation.AbstractComponent;
import org.apache.beam.runners.jstorm.translation.CommonInstance;
import org.apache.beam.runners.jstorm.translation.Executor;
import org.apache.beam.runners.jstorm.translation.ExecutorsBolt;
import org.apache.beam.runners.jstorm.translation.JStormPipelineTranslator;
import org.apache.beam.runners.jstorm.translation.Stream;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.TxExecutorsBolt;
import org.apache.beam.runners.jstorm.translation.TxUnboundedSourceSpout;
import org.apache.beam.runners.jstorm.translation.UnboundedSourceSpout;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that translates the {@link Pipeline} to a JStorm DAG and executes it
 * either locally or on a JStorm cluster.
 */
public class JStormRunner extends PipelineRunner<JStormRunnerResult> {


  private static final Logger LOG = LoggerFactory.getLogger(JStormRunner.class);

  private JStormPipelineOptions options;

  public JStormRunner(JStormPipelineOptions options) {
    this.options = options;
  }

  public static JStormRunner fromOptions(PipelineOptions options) {
    JStormPipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(JStormPipelineOptions.class, options);
    return new JStormRunner(pipelineOptions);
  }

  /**
   * Convert pipeline options to JStorm configuration format.
   * @param options
   * @return JStorm configuration
   */
  private Config convertPipelineOptionsToConfig(JStormPipelineOptions options) {
    Config config = new Config();
    if (options.getLocalMode()) {
      config.put(Config.STORM_CLUSTER_MODE, "local");
    } else {
      config.put(Config.STORM_CLUSTER_MODE, "distributed");
    }

    Config.setNumWorkers(config, options.getWorkerNumber());

    config.putAll(options.getTopologyConfig());

    // Setup config for runtime env
    config.put("worker.external", "beam");
    // We use "com.alibaba.jstorm.transactional" API for "at least once" and "exactly once",
    // so we don't need acker task for beam job any more, and set related number to 0.
    config.put("topology.acker.executors", 0);

    // Register serializers of Kryo
    GuavaUtilsSerializer.registerSerializers(config);
    BeamUtilsSerializer.registerSerializers(config);
    BeamSdkRepackUtilsSerializer.registerSerializers(config);
    JStormUtilsSerializer.registerSerializers(config);
    JavaUtilsSerializer.registerSerializers(config);
    return config;
  }

  @Override
  public JStormRunnerResult run(Pipeline pipeline) {
    LOG.info("Running pipeline...");
    TranslationContext context = new TranslationContext(this.options);
    JStormPipelineTranslator transformer = new JStormPipelineTranslator(context);
    transformer.translate(pipeline);
    LOG.info("UserGraphContext=\n{}", context.getUserGraphContext());
    LOG.info("ExecutionGraphContext=\n{}", context.getExecutionGraphContext());

    for (Stream stream : context.getExecutionGraphContext().getStreams()) {
      LOG.info(
          stream.getProducer().getComponentId() + " --> " + stream.getConsumer().getComponentId());
    }

    String topologyName = options.getJobName();
    Config config = convertPipelineOptionsToConfig(options);
    ConfigExtension.setTopologyComponentSubgraphDefinition(
        config, getSubGraphDefintions(context));

    return runTopology(
        topologyName,
        getTopology(options, context.getExecutionGraphContext()),
        config);
  }

  private JSONObject buildNode(String name, String type) {
    // Node: {name:name, type:tag/transform}
    JSONObject jsonNode = new JSONObject();
    jsonNode.put("name", name);
    jsonNode.put("type", type);
    return jsonNode;
  }

  private JSONArray buildEdge(Integer sourceId, Integer targetId) {
    JSONArray edge = new JSONArray();
    edge.addAll(Lists.newArrayList(sourceId, targetId));
    return edge;
  }

  private String getPValueName(TranslationContext.UserGraphContext userGraphContext,
                               TupleTag tupleTag) {
    PValue pValue = userGraphContext.findPValue(tupleTag);
    int index = pValue.getName().lastIndexOf("/");
    return pValue.getName().substring(index + 1);
  }

  private String getSubGraphDefintions(TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    TranslationContext.ExecutionGraphContext executionGraphContext =
        context.getExecutionGraphContext();
    JSONObject graph = new JSONObject();

    // Get sub-graphs for spouts
    for (Map.Entry<String, UnboundedSourceSpout> entry :
        executionGraphContext.getSpouts().entrySet()) {
      JSONObject subGraph = new JSONObject();

      // Nodes
      JSONObject nodes = new JSONObject();
      nodes.put(1, buildNode(entry.getValue().getName(), "transform"));
      nodes.put(2, buildNode(
          getPValueName(userGraphContext, entry.getValue().getOutputTag()), "tag"));
      subGraph.put("nodes", nodes);

      // Edges
      JSONArray edges = new JSONArray();
      edges.add(buildEdge(1, 2));
      subGraph.put("edges", edges);

      graph.put(entry.getKey(), subGraph);
    }

    // Get sub-graphs for bolts
    for (Map.Entry<String, ExecutorsBolt> entry : executionGraphContext.getBolts().entrySet()) {
      ExecutorsBolt executorsBolt = entry.getValue();
      Map<Executor, String> executorNames = executorsBolt.getExecutorNames();
      Map<TupleTag, Executor> inputTagToExecutors = executorsBolt.getExecutors();

      // Sub-Graph
      JSONObject subGraph = new JSONObject();

      // Nodes
      JSONObject nodes = new JSONObject();
      Map<String, Integer> nodeNameToId = Maps.newHashMap();
      int id = 1;
      for (Map.Entry<Executor, Collection<TupleTag>> entry1 :
          executorsBolt.getExecutorToOutputTags().entrySet()) {
        Executor executor = entry1.getKey();
        nodes.put(id, buildNode(executorNames.get(executor), "transform"));
        nodeNameToId.put(executorNames.get(executor), id);
        id++;
      }
      subGraph.put("nodes", nodes);

      Collection<TupleTag> externalOutputTags = executorsBolt.getExternalOutputTags();
      for (TupleTag outputTag : externalOutputTags) {
        String name = getPValueName(userGraphContext, outputTag);
        nodes.put(id, buildNode(name, "tag"));
        nodeNameToId.put(outputTag.getId(), id);
        id++;
      }

      Collection<TupleTag> externalInputTags = Sets.newHashSet(inputTagToExecutors.keySet());
      externalInputTags.removeAll(executorsBolt.getOutputTags());
      for (TupleTag inputTag : externalInputTags) {
        String name = getPValueName(userGraphContext, inputTag);
        nodes.put(id, buildNode(name, "tag"));
        nodeNameToId.put(inputTag.getId(), id);
        id++;
      }

      // Edges
      JSONArray edges = new JSONArray();
      for (Map.Entry<Executor, Collection<TupleTag>> entry1 :
          executorsBolt.getExecutorToOutputTags().entrySet()) {
        Executor sourceExecutor = entry1.getKey();
        Collection<TupleTag> outputTags = entry1.getValue();
        for (TupleTag tag : outputTags) {
          if (inputTagToExecutors.containsKey(tag)) {
            Executor targetExecutor = inputTagToExecutors.get(tag);
            if (executorNames.containsKey(targetExecutor)) {
              edges.add(buildEdge(nodeNameToId.get(executorNames.get(sourceExecutor)),
                  nodeNameToId.get(executorNames.get(targetExecutor))));
            }
          }
          if (externalOutputTags.contains(tag)) {
            edges.add(buildEdge(nodeNameToId.get(executorNames.get(sourceExecutor)),
                nodeNameToId.get(tag.getId())));
          }
        }
      }
      for (TupleTag tag : externalInputTags) {
        if (inputTagToExecutors.containsKey(tag)) {
          Executor targetExecutor = inputTagToExecutors.get(tag);
          if (executorNames.containsKey(targetExecutor)) {
            edges.add(buildEdge(nodeNameToId.get(tag.getId()),
                nodeNameToId.get(executorNames.get(targetExecutor))));
          }
        }
      }
      subGraph.put("edges", edges);

      graph.put(entry.getKey(), subGraph);
    }

    return graph.toJSONString();
  }

  private JStormRunnerResult runTopology(
      String topologyName,
      StormTopology topology,
      Config config) {
    try {
      if (StormConfig.local_mode(config)) {
        LocalCluster localCluster = LocalCluster.getInstance();
        localCluster.submitTopology(topologyName, config, topology);
        return JStormRunnerResult.local(
            topologyName, config, localCluster, options.getLocalModeExecuteTimeSec());
      } else {
        StormSubmitter.submitTopology(topologyName, config, topology);
        return null;
      }
    } catch (Exception e) {
      LOG.warn("Fail to submit topology", e);
      throw new RuntimeException("Fail to submit topology", e);
    }
  }

  private AbstractComponent getComponent(
      String id, TranslationContext.ExecutionGraphContext context) {
    AbstractComponent spout = context.getSpout(id);
    if (spout != null) {
      return spout;
    } else {
      return context.getBolt(id);
    }
  }

  private StormTopology getTopology(
      JStormPipelineOptions options, TranslationContext.ExecutionGraphContext context) {
    boolean isExactlyOnce = options.getExactlyOnceTopology();
    TopologyBuilder builder =
        isExactlyOnce ? new TransactionTopologyBuilder() : new TopologyBuilder();

    int parallelismNumber = options.getParallelism();
    Map<String, UnboundedSourceSpout> spouts = context.getSpouts();
    for (String id : spouts.keySet()) {
      IRichSpout spout = getSpout(isExactlyOnce, spouts.get(id));
      builder.setSpout(id, spout, getParallelismNum(spouts.get(id), parallelismNumber));
    }

    HashMap<String, BoltDeclarer> declarers = new HashMap<>();
    Iterable<Stream> streams = context.getStreams();
    LOG.info("streams=" + streams);
    for (Stream stream : streams) {
      String destBoltId = stream.getConsumer().getComponentId();
      IRichBolt bolt = getBolt(isExactlyOnce, context.getBolt(destBoltId));
      BoltDeclarer declarer = declarers.get(destBoltId);
      if (declarer == null) {
        declarer = builder.setBolt(
            destBoltId,
            bolt,
            getParallelismNum(context.getBolt(destBoltId), parallelismNumber));
        declarers.put(destBoltId, declarer);
      }

      Stream.Grouping grouping = stream.getConsumer().getGrouping();
      String streamId = stream.getProducer().getStreamId();
      String srcBoltId = stream.getProducer().getComponentId();

      // add stream output declare for "from" component
      AbstractComponent component = getComponent(srcBoltId, context);
      if (grouping.getType().equals(Stream.Grouping.Type.FIELDS)) {
        component.addKVOutputField(streamId);
      } else {
        component.addOutputField(streamId);
      }

      // "to" component declares grouping to "from" component
      switch (grouping.getType()) {
        case SHUFFLE:
          declarer.shuffleGrouping(srcBoltId, streamId);
          break;
        case FIELDS:
          declarer.fieldsGrouping(srcBoltId, streamId, new Fields(grouping.getFields()));
          break;
        case ALL:
          declarer.allGrouping(srcBoltId, streamId);
          break;
        case DIRECT:
          declarer.directGrouping(srcBoltId, streamId);
          break;
        case GLOBAL:
          declarer.globalGrouping(srcBoltId, streamId);
          break;
        case LOCAL_OR_SHUFFLE:
          declarer.localOrShuffleGrouping(srcBoltId, streamId);
          break;
        case NONE:
          declarer.noneGrouping(srcBoltId, streamId);
          break;
        default:
          throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
      }

      // Subscribe grouping of water mark stream
      component.addOutputField(CommonInstance.BEAM_WATERMARK_STREAM_ID);
      declarer.allGrouping(srcBoltId, CommonInstance.BEAM_WATERMARK_STREAM_ID);
    }

    if (isExactlyOnce) {
      ((TransactionTopologyBuilder) builder).enableHdfs();
    }
    return builder.createTopology();
  }

  private IRichSpout getSpout(boolean isExactlyOnce, IRichSpout spout) {
    IRichSpout ret = null;
    if (isExactlyOnce) {
      if (spout instanceof UnboundedSourceSpout) {
        ret = new TxUnboundedSourceSpout((UnboundedSourceSpout) spout);
      } else {
        String error = String.format(
            "The specified type(%s) is not supported in exactly once mode yet!",
            spout.getClass().toString());
        throw new RuntimeException(error);
      }
    } else {
      ret = spout;
    }
    return ret;
  }

  private IRichBolt getBolt(boolean isExactlyOnce, ExecutorsBolt bolt) {
    return isExactlyOnce ? new TxExecutorsBolt(bolt) : bolt;
  }

  /**
   * Calculate the final parallelism number according to the configured number and global number.
   *
   * @param component
   * @param globalParallelismNum
   * @return final parallelism number for the specified component
   */
  private int getParallelismNum(AbstractComponent component, int globalParallelismNum) {
    int configParallelismNum = component.getParallelismNum();
    return configParallelismNum > 0 ? configParallelismNum : globalParallelismNum;
  }
}
