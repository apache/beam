package org.apache.beam.runners.flink;

import com.google.auto.service.AutoService;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsRegistrar;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineRunnerRegistrar;
import com.google.common.collect.ImmutableList;


/**
 * AuteService registrar - will register FlinkRunner and FlinkOptions
 * as possible pipeline runner services.
 *
 * It ends up in META-INF/services and gets picked up by Dataflow.
 *
 */
public class FlinkRunnerRegistrar {
  private FlinkRunnerRegistrar() { }

  @AutoService(PipelineRunnerRegistrar.class)
  public static class Runner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>>of(FlinkPipelineRunner.class);
    }
  }

  @AutoService(PipelineOptionsRegistrar.class)
  public static class Options implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.<Class<? extends PipelineOptions>>of(FlinkPipelineOptions.class);
    }
  }
}
