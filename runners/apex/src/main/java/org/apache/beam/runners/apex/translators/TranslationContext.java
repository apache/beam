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
package org.apache.beam.runners.apex.translators;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translators.utils.CoderAdapterStreamCodec;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains context data for {@link TransformTranslator}s.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TranslationContext {

  private final ApexPipelineOptions pipelineOptions;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final Map<PCollection, Pair<OutputPort<?>, List<InputPort<?>>>> streams = new HashMap<>();
  private final Map<String, Operator> operators = new HashMap<>();

  public TranslationContext(ApexPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public void setCurrentTransform(TransformTreeNode treeNode) {
    this.currentTransform = AppliedPTransform.of(treeNode.getFullName(),
        treeNode.getInput(), treeNode.getOutput(), (PTransform) treeNode.getTransform());
  }

  public ApexPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public <InputT extends PInput> InputT getInput() {
    return (InputT) getCurrentTransform().getInput();
  }

  public <OutputT extends POutput> OutputT getOutput() {
    return (OutputT) getCurrentTransform().getOutput();
  }

  private AppliedPTransform<?, ?, ?> getCurrentTransform() {
    checkArgument(currentTransform != null, "current transform not set");
    return currentTransform;
  }

  public void addOperator(Operator operator, OutputPort port) {
    // Apex DAG requires a unique operator name
    // use the transform's name and make it unique
    String name = getCurrentTransform().getFullName();
    for (int i=1; this.operators.containsKey(name); name = getCurrentTransform().getFullName() + i++);
    this.operators.put(name, operator);
    PCollection<?> output = getOutput();
    this.streams.put(output, (Pair)new ImmutablePair<>(port, new ArrayList<>()));
  }

  /**
   * Add operator that is internal to a transformation.
   * @param output
   * @param operator
   * @param port
   * @param name
   */
  public <T> PInput addInternalOperator(Operator operator, OutputPort port, String name, Coder<T> coder) {
    checkArgument(this.operators.get(name) == null, "duplicate operator " + name);
    this.operators.put(name, operator);
    PCollection<T> input = getInput();
    PCollection<T> output = PCollection.createPrimitiveOutputInternal(input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
    output.setCoder(coder);
    this.streams.put(output, (Pair)new ImmutablePair<>(port, new ArrayList<>()));
    return output;
  }

  public void addStream(PInput input, InputPort inputPort) {
    Pair<OutputPort<?>, List<InputPort<?>>> stream = this.streams.get(input);
    checkArgument(stream != null, "no upstream operator defined");
    stream.getRight().add(inputPort);
  }

  public void populateDAG(DAG dag) {
    for (Map.Entry<String, Operator> nameAndOperator : this.operators.entrySet()) {
      dag.addOperator(nameAndOperator.getKey(), nameAndOperator.getValue());
    }
    int streamIndex = 0;
    for (Map.Entry<PCollection, Pair<OutputPort<?>, List<InputPort<?>>>> streamEntry : this.streams.entrySet()) {
      List<InputPort<?>> sinksList = streamEntry.getValue().getRight();
      InputPort[] sinks = sinksList.toArray(new InputPort[sinksList.size()]);
      if (sinks.length > 0) {
        dag.addStream("stream"+streamIndex++, streamEntry.getValue().getLeft(), sinks);
        for (InputPort port : sinks) {
          PCollection pc = streamEntry.getKey();
          Coder coder = pc.getCoder();
          if (pc.getWindowingStrategy() != null) {
            coder = FullWindowedValueCoder.of(pc.getCoder(),
                pc.getWindowingStrategy().getWindowFn().windowCoder()
                );
          }
          Coder<Object> wrapperCoder = ApexStreamTuple.ApexStreamTupleCoder.of(coder);
          CoderAdapterStreamCodec streamCodec = new CoderAdapterStreamCodec(wrapperCoder);
          dag.setInputPortAttribute(port, PortContext.STREAM_CODEC, streamCodec);
        }
      }
    }
  }

}
