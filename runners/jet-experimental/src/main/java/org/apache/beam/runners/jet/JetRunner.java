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
package org.apache.beam.runners.jet;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.jet.metrics.JetMetricsContainer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jet specific implementation of Beam's {@link PipelineRunner}. */
public class JetRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);
  private final JetPipelineOptions options;
  private final Function<ClientConfig, JetInstance> jetClientSupplier;

  private JetRunner(
      PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
    this.options = validate(options.as(JetPipelineOptions.class));
    this.jetClientSupplier = jetClientSupplier;
  }

  public static JetRunner fromOptions(PipelineOptions options) {
    return fromOptions(options, Jet::newJetClient);
  }

  public static JetRunner fromOptions(
      PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
    return new JetRunner(options, jetClientSupplier);
  }

  private static List<PTransformOverride> getDefaultOverrides() {
    //        return Collections.singletonList(JavaReadViaImpulse.boundedOverride()); //todo: needed
    // once we start using GreedyPipelineFuser
    return Collections.emptyList();
  }

  private static JetPipelineOptions validate(JetPipelineOptions options) {
    if (options.getJetGroupName() == null) {
      throw new IllegalArgumentException("Jet group name not set in options");
    }

    Integer localParallelism = options.getJetLocalParallelism();
    if (localParallelism == null) {
      throw new IllegalArgumentException("Jet node local parallelism must be specified");
    }
    if (localParallelism != -1 && localParallelism < 1) {
      throw new IllegalArgumentException("Jet node local parallelism must be >1 or -1");
    }

    return options;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    Boolean startOwnCluster = options.getJetStartOwnCluster();
    if (startOwnCluster) {
      Collection<JetInstance> jetInstances =
          Arrays.asList(Jet.newJetInstance(), Jet.newJetInstance());
      LOG.info("Started " + jetInstances.size() + " Jet cluster members");
    }
    try {
      return runInternal(pipeline);
    } finally {
      if (startOwnCluster) {
        Jet.shutdownAll();
        LOG.info("Stopped all Jet cluster members");
      }
    }
  }

  private PipelineResult runInternal(Pipeline pipeline) {
    try {
      normalize(pipeline);
      DAG dag = translate(pipeline);
      return run(dag);
    } catch (UnsupportedOperationException uoe) {
      LOG.error("Failed running pipeline!", uoe);
      return new FailedRunningPipelineResults(uoe);
    }
  }

  private void normalize(Pipeline pipeline) {
    pipeline.replaceAll(getDefaultOverrides());
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
  }

  private DAG translate(Pipeline pipeline) {
    JetGraphVisitor graphVisitor = new JetGraphVisitor(options);
    pipeline.traverseTopologically(graphVisitor);
    return graphVisitor.getDAG();
  }

  private JetPipelineResult run(DAG dag) {
    JetInstance jet = getJetInstance(options);

    IMapJet<String, MetricUpdates> metricsAccumulator =
        jet.getMap(JetMetricsContainer.METRICS_ACCUMULATOR_NAME);
    metricsAccumulator.clear();
    Job job = jet.newJob(dag);

    JetPipelineResult result = new JetPipelineResult(metricsAccumulator);
    result.setJob(job);

    job.join();
    result.setJob(null);
    job.cancel();
    jet.shutdown();

    return result;
  }

  private JetInstance getJetInstance(JetPipelineOptions options) {
    String jetGroupName = options.getJetGroupName();

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.getGroupConfig().setName(jetGroupName);
    return jetClientSupplier.apply(clientConfig);
  }
}
