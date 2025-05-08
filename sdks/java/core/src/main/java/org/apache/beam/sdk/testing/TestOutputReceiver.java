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
package org.apache.beam.sdk.testing;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.OutputBuilder;

/**
 * An implement of {@link DoFn.OutputReceiver} that naively collects all output values.
 *
 * <p>Because this API is crude and not designed to be very general, it is for internal use only and
 * will be changed arbitrarily.
 */
@Internal
public class TestOutputReceiver<T> implements DoFn.OutputReceiver<T> {
  private final List<T> records = new ArrayList<>();

  @Override
  public OutputBuilder<T> builder(T value) {
    return WindowedValue.builder(valueWithMetadata -> records.add(valueWithMetadata.getValue()));
  }

  public List<T> getOutputs() {
    return records;
  }
}
