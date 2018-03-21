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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.common.collect.Iterables;

final class MutationCellEstimator {
  // Prevent construction.
  private MutationCellEstimator() {
  }

  /**
   * Estimate the number of cells modified in a {@link MutationGroup}.
   */
  public static long countOf(SpannerSchema spannerSchema, MutationGroup mutationGroup) {
    long mutatedCells = 0L;
    for (Mutation mutation : mutationGroup) {
      if (mutation.getOperation() != Op.DELETE) {
        // sum the cells of the columns included in the mutation
        for (String column : mutation.getColumns()) {
          mutatedCells += spannerSchema.getCellsMutatedPerColumn(mutation.getTable(), column);
        }
      } else {
        // deletes are a little less obvious...
        // for single key deletes simply sum up all the columns in the schema
        // range deletes are broken up into batches already and can be ignored
        final KeySet keySet = mutation.getKeySet();

        final long rows = Iterables.size(keySet.getKeys());
        if (rows > 0) {
          mutatedCells += rows * spannerSchema.getCellsMutatedPerRow(mutation.getTable());
        }
      }
    }

    return mutatedCells;
  }
}
