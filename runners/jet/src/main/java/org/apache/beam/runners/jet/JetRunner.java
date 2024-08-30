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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.map.IMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.jet.metrics.JetMetricsContainer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.SplittableParDoNaiveBounded;
import org.apache.beam.sdk.util.construction.UnconsumedReads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jet specific implementation of Beam's {@link PipelineRunner}. */
public class JetRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);

  public static JetRunner fromOptions(PipelineOptions options) {
    return fromOptions(options, Jet::newJetClient);
  }

  public static JetRunner fromOptions(
      PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
    return new JetRunner(options, jetClientSupplier);
  }

  private final JetPipelineOptions options;
  private final Function<ClientConfig, JetInstance> jetClientSupplier;

  private Function<PTransform<?, ?>, JetTransformTranslator<?>> translatorProvider;

  private JetRunner(
      PipelineOptions options, Function<ClientConfig, JetInstance> jetClientSupplier) {
    this.options = validate(options.as(JetPipelineOptions.class));
    this.jetClientSupplier = jetClientSupplier;
    this.translatorProvider = JetTransformTranslators::getTranslator;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    try {
      normalize(pipeline);
      DAG dag = translate(pipeline);
      return run(dag);
    } catch (UnsupportedOperationException uoe) {
      LOG.error("Failed running pipeline!", uoe);
      return new FailedRunningPipelineResults(uoe);
    }
  }

  void addExtraTranslators(
      Function<PTransform<?, ?>, JetTransformTranslator<?>> extraTranslatorProvider) {
    Function<PTransform<?, ?>, JetTransformTranslator<?>> initialTranslatorProvider =
        this.translatorProvider;
    this.translatorProvider =
        transform -> {
          JetTransformTranslator<?> translator = initialTranslatorProvider.apply(transform);
          if (translator == null) {
            translator = extraTranslatorProvider.apply(transform);
          }
          return translator;
        };
  }

  private void normalize(Pipeline pipeline) {
    pipeline.replaceAll(getDefaultOverrides());
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
  }

  private DAG translate(Pipeline pipeline) {
    JetGraphVisitor graphVisitor = new JetGraphVisitor(options, translatorProvider);
    pipeline.traverseTopologically(graphVisitor);
    return graphVisitor.getDAG();
  }

  private JetPipelineResult run(DAG dag) {
    startClusterIfNeeded(options);

    JetInstance jet =
        getJetInstance(
            options); // todo: we use single client for each job, it might be better to have a
    // shared client with refcount

    Job job = jet.newJob(dag, getJobConfig(options));
    IMap<String, MetricUpdates> metricsAccumulator =
        jet.getMap(JetMetricsContainer.getMetricsMapName(job.getId()));
    JetPipelineResult pipelineResult = new JetPipelineResult(job, metricsAccumulator);
    CompletableFuture<Void> completionFuture =
        job.getFuture()
            .whenCompleteAsync(
                (r, f) -> {
                  pipelineResult.freeze(f);
                  metricsAccumulator.destroy();
                  jet.shutdown();

                  stopClusterIfNeeded(options);
                });
    pipelineResult.setCompletionFuture(completionFuture);

    return pipelineResult;
  }

  private void startClusterIfNeeded(JetPipelineOptions options) {
    Integer noOfLocalMembers = options.getJetLocalMode();
    if (noOfLocalMembers > 0) {
      Collection<JetInstance> jetInstances = new ArrayList<>();
      for (int i = 0; i < noOfLocalMembers; i++) {
        jetInstances.add(Jet.newJetInstance());
      }
      LOG.info("Started " + jetInstances.size() + " Jet cluster members");
    }
  }

  private void stopClusterIfNeeded(JetPipelineOptions options) {
    Integer noOfLocalMembers = options.getJetLocalMode();
    if (noOfLocalMembers > 0) {
      Jet.shutdownAll();
      LOG.info("Stopped all Jet cluster members");
    }
  }

  private JobConfig getJobConfig(JetPipelineOptions options) {
    JobConfig jobConfig = new JobConfig();

    String jobName = options.getJobName();
    if (jobName != null) {
      jobConfig.setName(jobName);
    }

    boolean hasNoLocalMembers = options.getJetLocalMode() <= 0;
    if (hasNoLocalMembers) {
      String codeJarPathname = options.getCodeJarPathname();
      if (codeJarPathname != null && !codeJarPathname.isEmpty()) {
        jobConfig.addJar(codeJarPathname);
      }
    }

    return jobConfig;
  }

  private JetInstance getJetInstance(JetPipelineOptions options) {
    String clusterName = options.getClusterName();

    ClientConfig clientConfig = new ClientConfig();
    clientConfig.setClusterName(clusterName);
    boolean hasNoLocalMembers = options.getJetLocalMode() <= 0;
    if (hasNoLocalMembers) {
      clientConfig
          .getNetworkConfig()
          .setAddresses(Arrays.asList(options.getJetServers().split(",")));
    }
    return jetClientSupplier.apply(clientConfig);
  }

  private static List<PTransformOverride> getDefaultOverrides() {
    return Arrays.asList(
        PTransformOverride.of(
            PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory<>()),
        PTransformOverride.of(
            PTransformMatchers.splittableProcessKeyedBounded(),
            new SplittableParDoNaiveBounded.OverrideFactory<>()),
        PTransformOverride.of(
            PTransformMatchers.splittableProcessKeyedUnbounded(),
            new SplittableParDoViaKeyedWorkItems.OverrideFactory<>()));
  }

  private static JetPipelineOptions validate(JetPipelineOptions options) {
    if (options.getClusterName() == null) {
      throw new IllegalArgumentException("Jet cluster name not set in options");
    }

    Integer localParallelism = options.getJetDefaultParallelism();
    if (localParallelism == null) {
      throw new IllegalArgumentException("Jet node local parallelism must be specified");
    }
    if (localParallelism != -1 && localParallelism < 1) {
      throw new IllegalArgumentException("Jet node local parallelism must be >1 or -1");
    }

    return options;
  }
}
