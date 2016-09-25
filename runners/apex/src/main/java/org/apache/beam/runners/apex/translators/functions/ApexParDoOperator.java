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

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translators.utils.NoOpStepContext;
import org.apache.beam.runners.apex.translators.utils.SerializablePipelineOptions;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.repackaged.com.google.common.base.Throwables;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.google.common.collect.Iterables;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Apex operator for Beam {@link DoFn}.
 */
public class ApexParDoOperator<InputT, OutputT> extends BaseOperator implements OutputManager {
  private static final Logger LOG = LoggerFactory.getLogger(ApexParDoOperator.class);
  private boolean traceTuples = true;

  private transient final TupleTag<OutputT> mainTag = new TupleTag<OutputT>();
  private transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackDoFnRunner;

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions pipelineOptions;
  @Bind(JavaSerializer.class)
  private final OldDoFn<InputT, OutputT> doFn;
  @Bind(JavaSerializer.class)
  private final WindowingStrategy<?, ?> windowingStrategy;
  @Bind(JavaSerializer.class)
  List<PCollectionView<?>> sideInputs;
// TODO: not Kryo serializable, integrate codec
//@Bind(JavaSerializer.class)
private transient StateInternals<Void> sideInputStateInternals = InMemoryStateInternals.forKey(null);
  private transient SideInputHandler sideInputHandler;
  // TODO: not Kryo serializable, integrate codec
  private List<WindowedValue<InputT>> pushedBack = new ArrayList<>();
  private LongMin pushedBackWatermark = new LongMin();
  private long currentInputWatermark = Long.MIN_VALUE;
  private long currentOutputWatermark = currentInputWatermark;

  public ApexParDoOperator(
      ApexPipelineOptions pipelineOptions,
      OldDoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      List<PCollectionView<?>> sideInputs
      )
  {
    this.pipelineOptions = new SerializablePipelineOptions(pipelineOptions);
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
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
        processWatermark((ApexStreamTuple.WatermarkTuple<?>)t);
      } else {
        if (traceTuples) {
          LOG.debug("\ninput {}\n", t.getValue());
        }
        Iterable<WindowedValue<InputT>> justPushedBack = processElementInReadyWindows(t.getValue());
        for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
          pushedBackWatermark.add(pushedBackValue.getTimestamp().getMillis());
          pushedBack.add(pushedBackValue);
        }
      }
    }
  };

  @InputPortFieldAnnotation(optional=true)
  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<Iterable<?>>>> sideInput1 = new DefaultInputPort<ApexStreamTuple<WindowedValue<Iterable<?>>>>()
  {
    private final int sideInputIndex = 0;

    @Override
    public void process(ApexStreamTuple<WindowedValue<Iterable<?>>> t)
    {
      if (t instanceof ApexStreamTuple.WatermarkTuple) {
        // ignore side input watermarks
        return;
      }
      if (traceTuples) {
        LOG.debug("\nsideInput {}\n", t.getValue());
      }
      PCollectionView<?> sideInput = sideInputs.get(sideInputIndex);
      sideInputHandler.addSideInputValue(sideInput, t.getValue());

      List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();
      for (WindowedValue<InputT> elem : pushedBack) {
        Iterable<WindowedValue<InputT>> justPushedBack = processElementInReadyWindows(elem);
        Iterables.addAll(newPushedBack, justPushedBack);
      }

      pushedBack.clear();
      pushedBackWatermark.clear();
      for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
        pushedBackWatermark.add(pushedBackValue.getTimestamp().getMillis());
        pushedBack.add(pushedBackValue);
      }

      // potentially emit watermark
      processWatermark(ApexStreamTuple.WatermarkTuple.of(currentInputWatermark));
    }
  };

  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<ApexStreamTuple<?>> output = new DefaultOutputPort<>();

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> tuple)
  {
    output.emit(ApexStreamTuple.DataTuple.of(tuple));
    if (traceTuples) {
      LOG.debug("\nemitting {}\n", tuple);
    }
  }

  private Iterable<WindowedValue<InputT>> processElementInReadyWindows(WindowedValue<InputT> elem) {
    try {
      return pushbackDoFnRunner.processElementInReadyWindows(elem);
    } catch (UserCodeException ue) {
      if (ue.getCause() instanceof AssertionError) {
        ApexRunner.assertionError = (AssertionError)ue.getCause();
      }
      throw ue;
    }
  }

  private void processWatermark(ApexStreamTuple.WatermarkTuple<?> mark)
  {
    this.currentInputWatermark = mark.getTimestamp();

    if (sideInputs.isEmpty()) {
      if (traceTuples) {
        LOG.debug("\nemitting watermark {}\n", mark);
      }
      output.emit(mark);
      return;
    }

    long potentialOutputWatermark =
        Math.min(pushedBackWatermark.get(), currentInputWatermark);
    if (potentialOutputWatermark > currentOutputWatermark) {
      currentOutputWatermark = potentialOutputWatermark;
      if (traceTuples) {
        LOG.debug("\nemitting watermark {}\n", currentOutputWatermark);
      }
      output.emit(ApexStreamTuple.WatermarkTuple.of(currentOutputWatermark));
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.traceTuples = ApexStreamTuple.Logging.isDebugEnabled(pipelineOptions.get(), this);
    SideInputReader sideInputReader = NullSideInputReader.of(sideInputs);
    if (!sideInputs.isEmpty()) {
      sideInputHandler = new SideInputHandler(sideInputs, sideInputStateInternals);
      sideInputReader = sideInputHandler;
    }

    DoFnRunner<InputT, OutputT> doFnRunner = DoFnRunners.createDefault(
        pipelineOptions.get(),
        doFn,
        sideInputReader,
        this,
        mainTag,
        TupleTagList.empty().getAll() /*sideOutputTags*/,
        new NoOpStepContext(),
        new NoOpAggregatorFactory(),
        windowingStrategy
        );

    pushbackDoFnRunner =
        PushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);

    try {
      doFn.setup();
    } catch (Exception e) {
      Throwables.propagate(e);
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    pushbackDoFnRunner.startBundle();
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
    pushbackDoFnRunner.finishBundle();
  }

  /**
   * TODO: Placeholder for aggregation, to be implemented for embedded and cluster mode.
   * It is called from {@link org.apache.beam.sdk.util.SimpleDoFnRunner}.
   */
  public static class NoOpAggregatorFactory implements AggregatorFactory {

    private NoOpAggregatorFactory() {
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
        Class<?> fnClass, ExecutionContext.StepContext step,
        String name, CombineFn<InputT, AccumT, OutputT> combine) {
      return new NoOpAggregator<InputT, OutputT>();
    }

    private static class NoOpAggregator<InputT, OutputT> implements Aggregator<InputT, OutputT>, java.io.Serializable
    {
      private static final long serialVersionUID = 1L;

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

  private static class LongMin {
    long state = Long.MAX_VALUE;

    public void add(long l) {
      state = Math.min(state, l);
    }

    public long get() {
      return state;
    }

    public void clear() {
      state = Long.MAX_VALUE;
    }

  }

}
