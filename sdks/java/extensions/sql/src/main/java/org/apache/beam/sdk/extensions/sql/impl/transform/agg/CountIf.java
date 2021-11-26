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
package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Combine;

/**
 * Returns the count of TRUE values for expression. Returns 0 if there are zero input rows, or if
 * expression evaluates to FALSE or NULL for all rows.
 */
public class CountIf {
  private CountIf() {}

  public static CountIfFn combineFn() {
    return new CountIf.CountIfFn();
  }

  public static class CountIfFn extends Combine.CombineFn<Boolean, CountIfFn.Accum, Long> {

    public static class Accum implements Serializable {
      boolean isExpressionFalse = true;
      long countIfResult = 0L;
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Boolean input) {
      if (input) {
        accum.isExpressionFalse = false;
        accum.countIfResult += 1;
      }
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      CountIfFn.Accum merged = createAccumulator();
      for (CountIfFn.Accum accum : accums) {
        if (!accum.isExpressionFalse) {
          merged.isExpressionFalse = false;
          merged.countIfResult += accum.countIfResult;
        }
      }
      return merged;
    }

    @Override
    public Long extractOutput(Accum accum) {
      if (!accum.isExpressionFalse) {
        return accum.countIfResult;
      }
      return 0L;
    }
  }
}
