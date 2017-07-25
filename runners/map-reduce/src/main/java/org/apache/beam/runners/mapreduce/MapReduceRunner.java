package org.apache.beam.runners.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Throwables;
import org.apache.beam.runners.mapreduce.translation.Graph;
import org.apache.beam.runners.mapreduce.translation.GraphConverter;
import org.apache.beam.runners.mapreduce.translation.GraphPlanner;
import org.apache.beam.runners.mapreduce.translation.JobPrototype;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * {@link PipelineRunner} for crunch.
 */
public class MapReduceRunner extends PipelineRunner<PipelineResult> {

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
    GraphConverter graphConverter = new GraphConverter();
    pipeline.traverseTopologically(graphConverter);

    Graph graph = graphConverter.getGraph();

    GraphPlanner planner = new GraphPlanner();
    Graph fusedGraph = planner.plan(graph);
    for (Graph.Vertex vertex : fusedGraph.getAllVertices()) {
      if (vertex.getStep().getTransform() instanceof GroupByKey) {
        JobPrototype jobPrototype = JobPrototype.create(1, vertex);
        try {
          Job job = jobPrototype.build(options.getJarClass(), new Configuration());
          job.waitForCompletion(true);
        } catch (Exception e) {
          Throwables.throwIfUnchecked(e);
        }
      }
    }
    return null;
  }
}
