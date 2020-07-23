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
package org.apache.beam.runners.twister2.translators.functions;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;

/** Output tag filter. */
public class OutputTagFilter<OutputT, InputT>
    implements ComputeCollectorFunc<WindowedValue<OutputT>, Iterator<RawUnionValue>> {

  private int tag;

  public OutputTagFilter() {
    // non arg constructor needed for kryo
  }

  public OutputTagFilter(int tag) {
    this.tag = tag;
  }

  @Override
  public void compute(
      Iterator<RawUnionValue> input, RecordCollector<WindowedValue<OutputT>> output) {
    RawUnionValue temp;
    while (input.hasNext()) {
      temp = input.next();
      if (temp.getUnionTag() == tag) {
        output.collect((WindowedValue<OutputT>) temp.getValue());
      }
    }
  }

  @Override
  public void prepare(TSetContext context) {}
}
