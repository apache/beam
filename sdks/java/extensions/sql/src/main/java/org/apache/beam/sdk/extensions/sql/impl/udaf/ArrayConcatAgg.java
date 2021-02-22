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

public class ArrayConcatAgg {
  public static class ArrayAggArray extends Combine.CombineFn<List<Object>, List, List> {

    @Override
    public List createAccumulator() {
      return new ArrayList();
    }

    @Override
    public List addInput(List accum, List<Object> input) {
      accum.addAll(input);
      return accum;
    }

    @Override
    public List mergeAccumulators(Iterable<List> accums) {
      List<Object> merged = new ArrayList();
      for (List<Object> accum : accums) {
        merged.add(accum);
      }
      return merged;
    }

    @Override
    public List extractOutput(List accum) {
      return accum;
    }
  }
}
