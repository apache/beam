/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.values;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link PDone} is the output of a {@link PTransform} that has a trivial result,
 * such as a {@link Write}.
 */
public class PDone extends POutputValueBase {

  /**
   * Creates a {@link PDone} in the given {@link Pipeline}.
   */
  public static PDone in(Pipeline pipeline) {
    return new PDone(pipeline);
  }

  @Override
  public Collection<? extends PValue> expand() {
    // A PDone contains no PValues.
    return Collections.emptyList();
  }

  private PDone(Pipeline pipeline) {
    super(pipeline);
  }
}
