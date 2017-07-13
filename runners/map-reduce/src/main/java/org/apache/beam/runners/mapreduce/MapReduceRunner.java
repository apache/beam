package org.apache.beam.runners.mapreduce;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;

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
    return new MapReduceRunner();
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    return null;
  }
}
