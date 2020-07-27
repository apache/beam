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
package org.apache.beam.runners.twister2;

import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Twister2TranslationContext. */
public abstract class Twister2TranslationContext {
  private final Twister2PipelineOptions options;
  protected final Map<PValue, TSet<?>> dataSets = new LinkedHashMap<>();
  private final Set<TSet> leaves = new LinkedHashSet<>();
  private final Map<String, BatchTSet<?>> sideInputDataSets;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final TSetEnvironment environment;

  public Twister2TranslationContext(Twister2PipelineOptions options) {
    this.options = options;
    this.environment = options.getTSetEnvironment();
    this.sideInputDataSets = new LinkedHashMap<>();
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return (T) Iterables.getOnlyElement(currentTransform.getOutputs().values());
  }

  public Twister2PipelineOptions getOptions() {
    return options;
  }

  public <T> void setOutputDataSet(PCollection<T> output, TSet<WindowedValue<T>> tset) {
    if (!dataSets.containsKey(output)) {
      dataSets.put(output, tset);
      leaves.add(tset);
    }
  }

  public <T> TSet<WindowedValue<T>> getInputDataSet(PValue input) {
    TSet<WindowedValue<T>> tSet = (TSet<WindowedValue<T>>) dataSets.get(input);
    leaves.remove(tSet);
    return tSet;
  }

  public <T> Map<TupleTag<?>, PValue> getInputs() {
    return currentTransform.getInputs();
  }

  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  public Map<TupleTag<?>, PValue> getOutputs() {
    return getCurrentTransform().getOutputs();
  }

  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return currentTransform.getOutputs().entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> ((PCollection) e.getValue()).getCoder()));
  }

  public TSetEnvironment getEnvironment() {
    return environment;
  }

  public abstract void eval(SinkTSet<?> tSet);

  public <ViewT, ElemT> void setSideInputDataSet(
      String value, BatchTSet<WindowedValue<ElemT>> set) {
    if (!sideInputDataSets.containsKey(value)) {
      sideInputDataSets.put(value, set);
    }
  }

  public Set<TSet> getLeaves() {
    return leaves;
  }

  public Map<String, BatchTSet<?>> getSideInputDataSets() {
    return sideInputDataSets;
  }
}
