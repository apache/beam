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
package com.google.cloud.dataflow.sdk.runners;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a simple {@link com.google.cloud.dataflow.sdk.Pipeline.PipelineVisitor}
 * that records the transformation tree.
 *
 * <p>Provided for internal unit tests.
 */
public class RecordingPipelineVisitor implements Pipeline.PipelineVisitor {

  public final List<PTransform<?, ?>> transforms = new ArrayList<>();
  public final List<PValue> values = new ArrayList<>();

  @Override
  public void enterCompositeTransform(TransformTreeNode node) {
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
  }

  @Override
  public void visitTransform(TransformTreeNode node) {
    transforms.add(node.getTransform());
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    values.add(value);
  }
}
