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
package org.apache.beam.runners.twister2.translators.functions;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.twister2.Twister2TranslationContext;
import org.apache.beam.runners.twister2.utils.NoOpStepContext;
import org.apache.beam.runners.twister2.utils.Twister2SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;

/** DoFn function. */
public class DoFnFunction<OutputT, InputT>
    implements ComputeCollectorFunc<RawUnionValue, Iterator<WindowedValue<InputT>>> {
  private static final Logger LOG = Logger.getLogger(DoFnFunction.class.getName());

  private transient DoFn<InputT, OutputT> doFn;
  private transient PipelineOptions pipelineOptions;
  private static final long serialVersionUID = -5701440128544343353L;
  private transient Coder<InputT> inputCoder;
  private transient Map<TupleTag<?>, Coder<?>> outputCoders;
  private transient WindowingStrategy<?, ?> windowingStrategy;
  private transient Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;
  private transient TupleTag<OutputT> mainOutput;
  private transient Twister2SideInputReader sideInputReader;
  private transient DoFnRunner<InputT, OutputT> doFnRunner;
  private transient DoFnOutputManager outputManager;
  private transient List<TupleTag<?>> sideOutputs;
  private StepContext stepcontext;
  private transient DoFnSchemaInformation doFnSchemaInformation;
  private transient Map<TupleTag<?>, Integer> outputMap;
  private transient Map<String, PCollectionView<?>> sideInputMapping;
  private transient DoFnInvoker doFnInvoker;

  // Byte arrays needed for serialization
  private transient boolean isInitialized = false;
  private transient RunnerApi.FunctionSpec doFnwithEx;
  private byte[] doFnwithExBytes;
  private byte[] coderBytes;
  private Map<String, byte[]> outputCodersBytes;
  private transient RunnerApi.MessageWithComponents windowStrategyProto;
  private byte[] windowBytes;
  private Map<String, byte[]> sideInputBytes;
  private String serializedOptions;
  private List<String> serializedSideOutputs;
  private Map<String, Integer> serializedOutputMap;

  public DoFnFunction() {
    // non arg constructor needed for kryo
    this.isInitialized = false;
  }

  public DoFnFunction(
      Twister2TranslationContext context,
      DoFn<InputT, OutputT> doFn,
      Coder<InputT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      List<TupleTag<?>> sideOutputs,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      TupleTag<OutputT> mainOutput,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<TupleTag<?>, Integer> outputMap,
      Map<String, PCollectionView<?>> sideInputMapping) {
    this.doFn = doFn;
    this.pipelineOptions = context.getOptions();
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions).toString();
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = convertToTuples(sideInputs);
    this.mainOutput = mainOutput;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideOutputs = sideOutputs;
    this.stepcontext = new NoOpStepContext();
    this.outputMap = outputMap;
    this.sideInputMapping = sideInputMapping;
    outputManager = new DoFnOutputManager(this.outputMap);
    prepareSerialization();
  }

  private Map<TupleTag<?>, WindowingStrategy<?, ?>> convertToTuples(
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs) {
    Map<TupleTag<?>, WindowingStrategy<?, ?>> temp = new HashMap<>();
    for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : sideInputs.entrySet()) {
      temp.put(entry.getKey().getTagInternal(), entry.getValue());
    }
    return temp;
  }

  /**
   * get the tag id's of all the keys.
   *
   * @return A Set of String key values.
   */
  public Set<String> getSideInputKeys() {
    initTransient();
    Set<String> keys = new HashSet<>();
    for (TupleTag<?> view : sideInputs.keySet()) {
      keys.add(view.getId());
    }
    return keys;
  }

  @Override
  public void prepare(TSetContext context) {
    initTransient();
    sideInputReader = new Twister2SideInputReader(sideInputs, context);
    outputManager.setup(mainOutput, sideOutputs);
    doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn);

    doFnRunner =
        DoFnRunners.simpleRunner(
            pipelineOptions,
            doFn,
            sideInputReader,
            outputManager,
            mainOutput,
            sideOutputs,
            stepcontext,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);
  }

  @Override
  public void compute(
      Iterator<WindowedValue<InputT>> input, RecordCollector<RawUnionValue> output) {
    try {
      outputManager.clear();
      doFnRunner.startBundle();
      while (input.hasNext()) {
        doFnRunner.processElement(input.next());
      }

      doFnRunner.finishBundle();
      Iterator<RawUnionValue> outputs = outputManager.getOutputs();
      while (outputs.hasNext()) {

        output.collect(outputs.next());
      }
    } catch (final RuntimeException re) {
      DoFnInvokers.invokerFor(doFn).invokeTeardown();
      throw re;
    }
  }

  @Override
  public void close() {
    Optional.ofNullable(doFnInvoker).ifPresent(DoFnInvoker::invokeTeardown);
  }

  private static class DoFnOutputManager implements DoFnRunners.OutputManager, Serializable {
    // todo need to figure out how this class types are handled
    private static final long serialVersionUID = 4967375172737408160L;
    private transient List<RawUnionValue> outputs;
    private transient Set<TupleTag<?>> outputTags;
    private Map<TupleTag<?>, Integer> outputMap;

    private DoFnOutputManager() {}

    DoFnOutputManager(Map<TupleTag<?>, Integer> outputMap) {
      this.outputMap = outputMap;
    }

    @Override
    public <T> void output(TupleTag<T> outputTag, WindowedValue<T> output) {
      if (outputTags.contains(outputTag)) {
        outputs.add(new RawUnionValue(outputMap.get(outputTag), output));
      }
    }

    void setup(TupleTag<?> mainOutput, List<TupleTag<?>> sideOutputs) {
      outputs = new ArrayList<>();
      outputTags = new HashSet<>();
      outputTags.add(mainOutput);
      outputTags.addAll(sideOutputs);
    }

    void clear() {
      outputs.clear();
    }

    Iterator<RawUnionValue> getOutputs() {
      return outputs.iterator();
    }
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }

  /**
   * prepares the DoFnFunction class so it can be serialized properly. This involves using various
   * protobuf's and byte arrays which are later converted back into the proper classes during
   * deserialization.
   */
  private void prepareSerialization() {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(
            pipelineOptions.as(PortablePipelineOptions.class)));
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions).toString();

    doFnwithEx =
        ParDoTranslation.translateDoFn(
            this.doFn, mainOutput, sideInputMapping, doFnSchemaInformation, components);
    doFnwithExBytes = doFnwithEx.getPayload().toByteArray();
    outputCodersBytes = new HashMap<>();
    try {
      coderBytes = SerializableUtils.serializeToByteArray(inputCoder);
      windowStrategyProto =
          WindowingStrategyTranslation.toMessageProto(windowingStrategy, components);
      windowBytes = windowStrategyProto.toByteArray();
      for (Map.Entry<TupleTag<?>, Coder<?>> entry : outputCoders.entrySet()) {
        outputCodersBytes.put(
            entry.getKey().getId(), SerializableUtils.serializeToByteArray(entry.getValue()));
      }
      sideInputBytes = new HashMap<>();
      for (Map.Entry<TupleTag<?>, WindowingStrategy<?, ?>> entry : sideInputs.entrySet()) {
        windowStrategyProto =
            WindowingStrategyTranslation.toMessageProto(entry.getValue(), components);
        sideInputBytes.put(entry.getKey().getId(), windowStrategyProto.toByteArray());
      }

      serializedSideOutputs = new ArrayList<>();
      for (TupleTag<?> sideOutput : sideOutputs) {
        serializedSideOutputs.add(sideOutput.getId());
      }

      serializedOutputMap = new HashMap<>();
      for (Map.Entry<TupleTag<?>, Integer> entry : outputMap.entrySet()) {
        serializedOutputMap.put(entry.getKey().getId(), entry.getValue());
      }
    } catch (IOException e) {
      LOG.info(e.getMessage());
    }
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }
    try {
      SdkComponents components = SdkComponents.create();

      pipelineOptions = new SerializablePipelineOptions(serializedOptions).get();
      DoFnWithExecutionInformation doFnWithExecutionInformation =
          (DoFnWithExecutionInformation)
              SerializableUtils.deserializeFromByteArray(doFnwithExBytes, "Custom Coder Bytes");
      this.doFn = (DoFn<InputT, OutputT>) doFnWithExecutionInformation.getDoFn();
      this.mainOutput = (TupleTag<OutputT>) doFnWithExecutionInformation.getMainOutputTag();
      this.sideInputMapping = doFnWithExecutionInformation.getSideInputMapping();
      this.doFnSchemaInformation = doFnWithExecutionInformation.getSchemaInformation();

      inputCoder =
          (Coder<InputT>)
              SerializableUtils.deserializeFromByteArray(coderBytes, "Custom Coder Bytes");

      windowStrategyProto = RunnerApi.MessageWithComponents.parseFrom(windowBytes);

      windowingStrategy =
          (WindowingStrategy<?, ?>)
              WindowingStrategyTranslation.fromProto(
                  windowStrategyProto.getWindowingStrategy(),
                  RehydratedComponents.forComponents(components.toComponents()));
      sideInputs = new HashMap<>();
      for (Map.Entry<String, byte[]> entry : sideInputBytes.entrySet()) {
        windowStrategyProto = RunnerApi.MessageWithComponents.parseFrom(entry.getValue());
        sideInputs.put(
            new TupleTag<>(entry.getKey()),
            WindowingStrategyTranslation.fromProto(
                windowStrategyProto.getWindowingStrategy(),
                RehydratedComponents.forComponents(components.toComponents())));
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.info(e.getMessage());
    }
    outputCoders = new HashMap<>();
    for (Map.Entry<String, byte[]> entry : outputCodersBytes.entrySet()) {
      outputCoders.put(
          new TupleTag<>(entry.getKey()),
          (Coder<?>)
              SerializableUtils.deserializeFromByteArray(entry.getValue(), "Custom Coder Bytes"));
    }

    sideOutputs = new ArrayList<>();
    for (String sideOutput : serializedSideOutputs) {
      sideOutputs.add(new TupleTag<>(sideOutput));
    }

    outputMap = new HashMap<>();
    for (Map.Entry<String, Integer> entry : serializedOutputMap.entrySet()) {
      outputMap.put(new TupleTag<>(entry.getKey()), entry.getValue());
    }
    outputManager = new DoFnOutputManager(this.outputMap);
    this.isInitialized = true;
  }
}
