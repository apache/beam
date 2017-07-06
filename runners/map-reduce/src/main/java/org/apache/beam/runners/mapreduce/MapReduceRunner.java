package org.apache.beam.runners.mapreduce;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;

/**
 * {@link PipelineRunner} for crunch.
 */
public class MapReduceRunner extends PipelineRunner<PipelineResult> {
  @Override
  public PipelineResult run(Pipeline pipeline) {
    return null;
  }
}
