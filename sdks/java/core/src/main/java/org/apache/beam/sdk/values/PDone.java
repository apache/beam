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
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * {@link PDone} is the output of a {@link PTransform} that has a trivial result, such as a {@link
 * WriteFiles}.
 */
public class PDone implements POutput {

  private final Pipeline pipeline;

  private PDone(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  /** Creates a {@link PDone} in the given {@link Pipeline}. */
  public static PDone in(Pipeline pipeline) {
    return new PDone(pipeline);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  /** A {@link PDone} contains no {@link PValue PValues}. */
  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return Collections.emptyMap();
  }

  /** Does nothing; there is nothing to finish specifying. */
  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
