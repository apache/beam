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

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Iterables;

final class MutationUtils {
  private MutationUtils() {

  }

  /**
   * Check if the mutation is a delete by a single primary key operation.
   * @param m mutation
   * @return true if mutation is a point delete
   */
  public static boolean isPointDelete(Mutation m) {
    return m.getOperation() == Mutation.Op.DELETE && Iterables.isEmpty(m.getKeySet().getRanges())
        && Iterables.size(m.getKeySet().getKeys()) == 1;
  }

}
