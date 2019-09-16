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
package org.apache.beam.sdk.values;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO.Read;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link PBegin} is the "input" to a root {@link PTransform}, such as {@link Read Read} or {@link
 * org.apache.beam.sdk.transforms.Create}.
 *
 * <p>Typically elided by simply calling {@link Pipeline#apply(String, PTransform)} or {@link
 * Pipeline#apply(PTransform)}, but one can be explicitly created by calling {@link Pipeline#begin}
 * on a Pipeline.
 */
public class PBegin implements PInput {
  /** Returns a {@link PBegin} in the given {@link Pipeline}. */
  public static PBegin in(Pipeline pipeline) {
    return new PBegin(pipeline);
  }

  /**
   * Like {@link #apply(String, PTransform)} but defaulting to the name of the {@link PTransform}.
   */
  public <OutputT extends POutput> OutputT apply(PTransform<? super PBegin, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this {@link PBegin}, using {@code name} to identify
   * this specific application of the transform.
   *
   * <p>This name is used in various places, including the monitoring UI, logging, and to stably
   * identify this application node in the job graph.
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PBegin, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    // A PBegin contains no PValues.
    return Collections.emptyMap();
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Constructs a {@link PBegin} in the given {@link Pipeline}. */
  protected PBegin(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  private final Pipeline pipeline;
}
