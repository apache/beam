/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PValue;

import java.util.HashSet;
import java.util.Set;

/**
 * A pipeline visitor that tracks all keyed values.
 */
class KeyedPValueTrackingVisitor implements PipelineVisitor {
  @SuppressWarnings("rawtypes")
  private final Set<Class<? extends PTransform>> producesKeyedOutputs;
  private final Set<PValue> keyedValues;

  public static KeyedPValueTrackingVisitor create(
      @SuppressWarnings("rawtypes") Set<Class<? extends PTransform>> producesKeyedOutputs) {
    return new KeyedPValueTrackingVisitor(producesKeyedOutputs);
  }

  private KeyedPValueTrackingVisitor(
      @SuppressWarnings("rawtypes") Set<Class<? extends PTransform>> producesKeyedOutputs) {
    this.producesKeyedOutputs = producesKeyedOutputs;
    this.keyedValues = new HashSet<>();
  }

  @Override
  public void enterCompositeTransform(TransformTreeNode node) {}

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {}

  @Override
  public void visitTransform(TransformTreeNode node) {}

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    if (producesKeyedOutputs.contains(producer.getTransform().getClass())) {
      keyedValues.addAll(value.expand());
    } else if (!producer.getInput().expand().isEmpty()
        && keyedValues.containsAll(producer.getInput().expand())) {
      keyedValues.addAll(value.expand());
    }
  }

  public Set<PValue> getKeyedPValues() {
    return keyedValues;
  }
}

