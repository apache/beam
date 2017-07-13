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
import com.alibaba.jstorm.cache.KvStoreIterable;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.transactional.TransactionTopologyBuilder;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.jstorm.serialization.ImmutableListSerializer;
import org.apache.beam.runners.jstorm.serialization.ImmutableMapSerializer;
import org.apache.beam.runners.jstorm.serialization.ImmutableSetSerializer;
import org.apache.beam.runners.jstorm.serialization.KvStoreIterableSerializer;
import org.apache.beam.runners.jstorm.serialization.SdkRepackImmuListSerializer;
import org.apache.beam.runners.jstorm.serialization.SdkRepackImmuSetSerializer;
import org.apache.beam.runners.jstorm.serialization.UnmodifiableCollectionsSerializer;
import org.apache.beam.runners.jstorm.translation.StormPipelineTranslator;
import org.apache.beam.runners.jstorm.translation.TranslationContext;
import org.apache.beam.runners.jstorm.translation.runtime.AbstractComponent;
import org.apache.beam.runners.jstorm.translation.runtime.AdaptorBasicBolt;
import org.apache.beam.runners.jstorm.translation.runtime.AdaptorBasicSpout;
import org.apache.beam.runners.jstorm.translation.runtime.ExecutorsBolt;
import org.apache.beam.runners.jstorm.translation.runtime.TxExecutorsBolt;
import org.apache.beam.runners.jstorm.translation.runtime.TxUnboundedSourceSpout;
import org.apache.beam.runners.jstorm.translation.runtime.UnboundedSourceSpout;
import org.apache.beam.runners.jstorm.translation.translator.Stream;
import org.apache.beam.runners.jstorm.translation.util.CommonInstance;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
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
    JStormPipelineOptions pipelineOptions = PipelineOptionsValidator.validate(
        JStormPipelineOptions.class, options);
    return new JStormRunner(pipelineOptions);
  }

  /**
   * convert pipeline options to storm configuration format
   *
   * @param options
   * @return
   */
  private Config convertPipelineOptionsToConfig(JStormPipelineOptions options) {
    Config config = new Config();
    if (options.getLocalMode())
      config.put(Config.STORM_CLUSTER_MODE, "local");
    else
      config.put(Config.STORM_CLUSTER_MODE, "distributed");

    Config.setNumWorkers(config, options.getWorkerNumber());

    config.putAll(options.getTopologyConfig());

    // Setup config for runtime env
    config.put("worker.external", "beam");
    config.put("topology.acker.executors", 0);

    UnmodifiableCollectionsSerializer.registerSerializers(config);
    // register classes of guava utils, ImmutableList, ImmutableSet, ImmutableMap
    ImmutableListSerializer.registerSerializers(config);
    SdkRepackImmuListSerializer.registerSerializers(config);
    ImmutableSetSerializer.registerSerializers(config);
    SdkRepackImmuSetSerializer.registerSerializers(config);
    ImmutableMapSerializer.registerSerializers(config);

    config.registerDefaultSerailizer(KvStoreIterable.class, KvStoreIterableSerializer.class);
    return config;
  }

  @Override
  public JStormRunnerResult run(Pipeline pipeline) {
    LOG.info("Running pipeline...");
    TranslationContext context = new TranslationContext(this.options);
    StormPipelineTranslator transformer = new StormPipelineTranslator(context);
    transformer.translate(pipeline);
    LOG.info("UserGraphContext=\n{}", context.getUserGraphContext());
    LOG.info("ExecutionGraphContext=\n{}", context.getExecutionGraphContext());

    for (Stream stream : context.getExecutionGraphContext().getStreams()) {
      LOG.info(
          stream.getProducer().getComponentId() + " --> " + stream.getConsumer().getComponentId());
    }

    String topologyName = options.getJobName();
    Config config = convertPipelineOptionsToConfig(options);

    return runTopology(
        topologyName,
        getTopology(options, context.getExecutionGraphContext()),
        config);
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
            topologyName, config, localCluster, options.getLocalModeExecuteTime());
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
    AbstractComponent component = null;
    AdaptorBasicSpout spout = context.getSpout(id);
    if (spout != null) {
      component = spout;
    } else {
      AdaptorBasicBolt bolt = context.getBolt(id);
      if (bolt != null)
        component = bolt;
    }

    return component;
  }

  private StormTopology getTopology(
      JStormPipelineOptions options, TranslationContext.ExecutionGraphContext context) {
    boolean isExactlyOnce = options.getExactlyOnceTopology();
    TopologyBuilder builder =
        isExactlyOnce ? new TransactionTopologyBuilder() : new TopologyBuilder();

    int parallelismNumber = options.getParallelismNumber();
    Map<String, AdaptorBasicSpout> spouts = context.getSpouts();
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
      if (grouping.getType().equals(Stream.Grouping.Type.FIELDS))
        component.addKVOutputField(streamId);
      else
        component.addOutputField(streamId);

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
