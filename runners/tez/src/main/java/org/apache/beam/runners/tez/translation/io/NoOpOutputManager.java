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
package org.apache.beam.runners.tez.translation.io;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * {@link TezOutputManager} implementation for when the {@link org.apache.tez.dag.api.Vertex} has no output.
 * Used in cases such as when the ParDo within the Vertex writes the output itself.
 */
public class NoOpOutputManager extends TezOutputManager {

  public NoOpOutputManager() {
    super(null);
  }

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {}
}
