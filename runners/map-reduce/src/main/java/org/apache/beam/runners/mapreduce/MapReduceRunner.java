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
package org.apache.beam.runners.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.mapreduce.translation.DotfileWriter;
import org.apache.beam.runners.mapreduce.translation.GraphConverter;
import org.apache.beam.runners.mapreduce.translation.GraphPlanner;
import org.apache.beam.runners.mapreduce.translation.Graphs;
import org.apache.beam.runners.mapreduce.translation.JobPrototype;
import org.apache.beam.runners.mapreduce.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PipelineRunner} for MapReduce.
 */
public class MapReduceRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceRunner.class);

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static MapReduceRunner fromOptions(PipelineOptions options) {
    return new MapReduceRunner(options.as(MapReducePipelineOptions.class));
  }

  private final MapReducePipelineOptions options;

  MapReduceRunner(MapReducePipelineOptions options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    BasicConfigurator.configure();
    MetricsEnvironment.setMetricsSupported(true);

    TranslationContext context = new TranslationContext(options);
    GraphConverter graphConverter = new GraphConverter(context);
    pipeline.traverseTopologically(graphConverter);

    LOG.info(graphConverter.getDotfile());

    Graphs.FusedGraph fusedGraph = new Graphs.FusedGraph(context.getInitGraph());
    LOG.info(DotfileWriter.toDotfile(fusedGraph));

    GraphPlanner planner = new GraphPlanner(options);
    fusedGraph = planner.plan(fusedGraph);

    LOG.info(DotfileWriter.toDotfile(fusedGraph));

    fusedGraph.getFusedSteps();

    List<Job> jobs = new ArrayList<>();
    int stageId = 0;
    for (Graphs.FusedStep fusedStep : fusedGraph.getFusedSteps()) {
      Configuration config = new Configuration();
      config.set("keep.failed.task.files", "true");

      JobPrototype jobPrototype = JobPrototype.create(stageId++, fusedStep, options);
      LOG.info("Running job-{}.", stageId);
      LOG.info(DotfileWriter.toDotfile(fusedStep));
      try {
        Job job = jobPrototype.build(options.getJarClass(), config);
        job.waitForCompletion(true);
        if (!job.getStatus().getState().equals(JobStatus.State.SUCCEEDED)) {
          throw new RuntimeException("MapReduce job failed: " + job.getJobID());
        }
        jobs.add(job);
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
    return new MapReducePipelineResult(jobs);
  }
}
