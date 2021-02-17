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
package org.apache.beam.sdk.extensions.sql.impl.udaf;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine;

public class ArrayAgg {

  public static class ArrayAggArray extends Combine.CombineFn<Object, List<Object>, Object[]> {
    @Override
    public List<Object> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<Object> addInput(List<Object> accum, Object input) {
      accum.add(input);
      return accum;
    }

    @Override
    public List<Object> mergeAccumulators(Iterable<List<Object>> accums) {
      List<Object> merged = new ArrayList<>();
      for (List<Object> accum : accums) {
        for (Object o : accum) {
          merged.add(o);
        }
      }
      return merged;
    }

    @Override
    public Object[] extractOutput(List<Object> accumulator) {
      return accumulator.toArray();
    }
  }
}
