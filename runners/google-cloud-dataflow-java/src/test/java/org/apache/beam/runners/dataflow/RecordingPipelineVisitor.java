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
package org.apache.beam.runners.dataflow;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

/**
 * Provides a simple {@link org.apache.beam.sdk.Pipeline.PipelineVisitor} that records the
 * transformation tree.
 */
class RecordingPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  public final List<PTransform<?, ?>> transforms = new ArrayList<>();
  public final List<PValue> values = new ArrayList<>();

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    transforms.add(node.getTransform());
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    values.add(value);
  }
}
