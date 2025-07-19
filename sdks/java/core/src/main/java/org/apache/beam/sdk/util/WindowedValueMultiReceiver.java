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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;

/**
 * Encapsulation of a method of output that can output a value with all of its windowing information
 * to a tagged destination.
 */
@Internal
public interface WindowedValueMultiReceiver {
  /**
   * Outputs to the given {@link TupleTag}.
   *
   * <p>Sometiems it is useful to fix a tag to produce a {@link WindowedValueReceiver}. To do so,
   * use a lambda to curry this method, as in {@code value -> receiver.output(tag, value)}.
   */
  <OutputT> void output(TupleTag<OutputT> tag, WindowedValue<OutputT> value);
}
