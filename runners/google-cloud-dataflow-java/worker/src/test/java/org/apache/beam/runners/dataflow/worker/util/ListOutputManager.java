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
package org.apache.beam.runners.dataflow.worker.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * An implementation of {@code OutputManager} using simple lists, for testing and in-memory contexts
 * such as the {@code DirectRunner}.
 */
public class ListOutputManager implements OutputManager {

  private Map<TupleTag<?>, List<WindowedValue<?>>> outputLists = Maps.newHashMap();

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    @SuppressWarnings({"rawtypes", "unchecked"})
    List<WindowedValue<T>> outputList = (List) outputLists.get(tag);

    if (outputList == null) {
      outputList = Lists.newArrayList();
      @SuppressWarnings({"rawtypes", "unchecked"})
      List<WindowedValue<?>> untypedList = (List) outputList;
      outputLists.put(tag, untypedList);
    }

    outputList.add(output);
  }

  public <T> List<WindowedValue<T>> getOutput(TupleTag<T> tag) {
    // Safe cast by design, inexpressible in Java without rawtypes
    @SuppressWarnings({"rawtypes", "unchecked"})
    List<WindowedValue<T>> outputList = (List) outputLists.get(tag);
    return (outputList != null) ? outputList : Collections.<WindowedValue<T>>emptyList();
  }
}
