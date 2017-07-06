package org.apache.beam.runners.mapreduce;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

/**
 * Registrars for {@link MapReduceRunner}.
 */
public class MapReduceRegistrar {
  private MapReduceRegistrar() {
  }

  @AutoService(PipelineRunnerRegistrar.class)
  public static class Runner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>> of(MapReduceRunner.class);
    }
  }

  @AutoService(PipelineOptionsRegistrar.class)
  public static class Options implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>> of(MapReducePipelineOptions.class);
    }
  }
}
