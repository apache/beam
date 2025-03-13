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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PValue;

/** Test utilities for the {@link DirectRunner}. */
public final class DirectGraphs {
  public static void performDirectOverrides(Pipeline p) {
    DirectRunner runner =
        DirectRunner.fromOptions(PipelineOptionsFactory.create().as(DirectOptions.class));
    runner.performRewrites(p);
  }

  public static DirectGraph getGraph(Pipeline p) {
    DirectGraphVisitor visitor = new DirectGraphVisitor();
    p.traverseTopologically(visitor);
    return visitor.getGraph();
  }

  public static AppliedPTransform<?, ?, ?> getProducer(PValue value) {
    return getGraph(value.getPipeline()).getProducer(value);
  }
}
