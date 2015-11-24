/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import java.util.Collection;

/**
 * An internal class for extracting {@link Aggregator Aggregators} from {@link DoFn DoFns}.
 */
public final class AggregatorRetriever {
  private AggregatorRetriever() {
    // do not instantiate
  }

  /**
   * Returns the {@link Aggregator Aggregators} created by the provided {@link DoFn}.
   */
  public static Collection<Aggregator<?, ?>> getAggregators(DoFn<?, ?> fn) {
    return fn.getAggregators();
  }
}

