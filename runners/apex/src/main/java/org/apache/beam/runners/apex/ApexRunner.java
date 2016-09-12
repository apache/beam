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
package org.apache.beam.runners.apex;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.apex.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.runners.core.AssignWindows;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.google.common.base.Throwables;

/**
 * A {@link PipelineRunner} that translates the
 * pipeline to an Apex DAG and executes it on an Apex cluster.
 * <p>
 * Currently execution is always in embedded mode,
 * launch on Hadoop cluster will be added in subsequent iteration.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ApexRunner extends PipelineRunner<ApexRunnerResult> {

  private final ApexPipelineOptions options;

  public ApexRunner(ApexPipelineOptions options) {
    this.options = options;
  }

  public static ApexRunner fromOptions(PipelineOptions options) {
    return new ApexRunner((ApexPipelineOptions) options);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
//System.out.println("transform: " + transform);

    if (Window.Bound.class.equals(transform.getClass())) {
      return (OutputT) ((PCollection) input).apply(
          new AssignWindowsAndSetStrategy((Window.Bound) transform));
    } else if (Create.Values.class.equals(transform.getClass())) {
      return (OutputT) PCollection
          .<OutputT>createPrimitiveOutputInternal(
              input.getPipeline(),
              WindowingStrategy.globalDefault(),
              PCollection.IsBounded.BOUNDED);
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public ApexRunnerResult run(Pipeline pipeline) {

    final TranslationContext translationContext = new TranslationContext(options);
    ApexPipelineTranslator translator = new ApexPipelineTranslator(translationContext);
    translator.translate(pipeline);

    StreamingApplication apexApp = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
        dag.setAttribute(DAGContext.APPLICATION_NAME, options.getApplicationName());
        translationContext.populateDAG(dag);
      }
    };

    checkArgument(options.isEmbeddedExecution(), "only embedded execution is supported at this time");
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    try {
      lma.prepareDAG(apexApp, conf);
      LocalMode.Controller lc = lma.getController();
      if (options.isEmbeddedExecutionDebugMode()) {
        // turns off timeout checking for operator progress
        lc.setHeartbeatMonitoringEnabled(false);
      }
      if (options.getRunMillis() > 0) {
        lc.run(options.getRunMillis());
      } else {
        lc.runAsync();
      }
      return new ApexRunnerResult(lma.getDAG(), lc);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * copied from DirectPipelineRunner.
   * used to replace Window.Bound till equivalent function is added in Apex
   */
  private static class AssignWindowsAndSetStrategy<T, W extends BoundedWindow>
      extends PTransform<PCollection<T>, PCollection<T>> {

    private final Window.Bound<T> wrapped;

    public AssignWindowsAndSetStrategy(Window.Bound<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public PCollection<T> apply(PCollection<T> input) {
      WindowingStrategy<?, ?> outputStrategy =
          wrapped.getOutputStrategyInternal(input.getWindowingStrategy());

      WindowFn<T, BoundedWindow> windowFn =
          (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();

      // If the Window.Bound transform only changed parts other than the WindowFn, then
      // we skip AssignWindows even though it should be harmless in a perfect world.
      // The world is not perfect, and a GBK may have set it to InvalidWindows to forcibly
      // crash if another GBK is performed without explicitly setting the WindowFn. So we skip
      // AssignWindows in this case.
      if (wrapped.getWindowFn() == null) {
        return input.apply("Identity", ParDo.of(new IdentityFn<T>()))
            .setWindowingStrategyInternal(outputStrategy);
      } else {
        return input
            .apply("AssignWindows", new AssignWindows<>(windowFn))
            .setWindowingStrategyInternal(outputStrategy);
      }
    }
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

}
