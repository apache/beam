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
package org.apache.beam.runners.flink.translation.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;

/** Pipeline visitors that fills a lookup table of {@link PValue} to number of consumers. */
public class CountingPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

  private final Map<PValue, Integer> numConsumers = new HashMap<>();

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    for (PValue input : node.getInputs().values()) {
      numConsumers.merge(input, 1, Integer::sum);
    }
  }

  /**
   * Calculate number of consumers of a given {@link PValue}.
   *
   * @param value PValue to perform calculation for.
   * @return Number of consumers.
   */
  public int getNumConsumers(PValue value) {
    return numConsumers.get(value);
  }
}
