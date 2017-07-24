package org.apache.beam.runners.mapreduce;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link PipelineOptions} for {@link MapReduceRunner}.
 */
public interface MapReducePipelineOptions extends PipelineOptions {

  @Description("The jar class of the user Beam program.")
  Class<?> getJarClass();
  void setJarClass(Class<?> jarClass);
}
