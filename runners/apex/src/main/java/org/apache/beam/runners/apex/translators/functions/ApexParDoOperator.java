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

package org.apache.beam.runners.apex.translators.functions;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translators.utils.NoOpStepContext;
import org.apache.beam.runners.apex.translators.utils.SerializablePipelineOptions;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Apex operator for Beam {@link DoFn}.
 */
public class ApexParDoOperator<InputT, OutputT> extends BaseOperator implements OutputManager {

  private transient final TupleTag<OutputT> mainTag = new TupleTag<OutputT>();
  private transient DoFnRunner<InputT, OutputT> doFnRunner;

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions pipelineOptions;
  @Bind(JavaSerializer.class)
  private final OldDoFn<InputT, OutputT> doFn;
  @Bind(JavaSerializer.class)
  private final WindowingStrategy<?, ?> windowingStrategy;
  @Bind(JavaSerializer.class)
  private final SideInputReader sideInputReader;

  public ApexParDoOperator(
      ApexPipelineOptions pipelineOptions,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      SideInputReader sideInputReader) {
    this.pipelineOptions = new SerializablePipelineOptions(pipelineOptions);
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputReader = sideInputReader;
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexParDoOperator() {
    this(null, null, null, null);
  }

  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>> input = new DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>>()
  {
    @Override
    public void process(ApexStreamTuple<WindowedValue<InputT>> t)
    {
      if (t instanceof ApexStreamTuple.WatermarkTuple) {
        output.emit(t);
      } else {
        System.out.println("\n" + Thread.currentThread().getName() + "\n" + t.getValue() + "\n");
        doFnRunner.processElement(t.getValue());
      }
    }
  };

  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<ApexStreamTuple<?>> output = new DefaultOutputPort<>();

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> tuple)
  {
    output.emit(ApexStreamTuple.DataTuple.of(tuple));
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.doFnRunner = DoFnRunners.simpleRunner(pipelineOptions.get(),
        doFn,
        sideInputReader,
        this,
        mainTag,
        TupleTagList.empty().getAll(),
        new NoOpStepContext(),
        new NoOpAggregatorFactory(),
        windowingStrategy
        );
  }

  @Override
  public void beginWindow(long windowId)
  {
    doFnRunner.startBundle();
    /*
    Collection<Aggregator<?, ?>> aggregators = AggregatorRetriever.getAggregators(doFn);
    if (!aggregators.isEmpty()) {
      System.out.println("\n" + Thread.currentThread().getName() + "\n" +AggregatorRetriever.getAggregators(doFn) + "\n");
    }
    */
  }

  @Override
  public void endWindow()
  {
    doFnRunner.finishBundle();
  }

  /**
   * TODO: Placeholder for aggregation, to be implemented for embedded and cluster mode.
   * It is called from {@link org.apache.beam.sdk.util.SimpleDoFnRunner}.
   */
  public class NoOpAggregatorFactory implements AggregatorFactory {

    private NoOpAggregatorFactory() {
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass, ExecutionContext.StepContext step,
        String name, CombineFn<InputT, AccumT, OutputT> combine) {
      return new Aggregator<InputT, OutputT>() {

        @Override
        public void addValue(InputT value)
        {
        }

        @Override
        public String getName()
        {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public CombineFn<InputT, ?, OutputT> getCombineFn()
        {
          // TODO Auto-generated method stub
          return null;
        }

      };
    }
  }



}
