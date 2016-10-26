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
package org.apache.beam.runners.direct;

import java.util.Map;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * An {@link OutputManager} that outputs to {@link CommittedBundle Bundles} used by the
 * {@link DirectRunner}.
 */
class UncommittedBundleOutputManager implements OutputManager {
  private final Map<TupleTag<?>, UncommittedBundle<?>> bundles;

  public static UncommittedBundleOutputManager create(
      Map<TupleTag<?>, UncommittedBundle<?>> outputBundles) {
    return new UncommittedBundleOutputManager(outputBundles);
  }

  UncommittedBundleOutputManager(Map<TupleTag<?>, UncommittedBundle<?>> bundles) {
    this.bundles = bundles;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
    @SuppressWarnings("rawtypes")
    UncommittedBundle bundle = bundles.get(tag);
    bundle.add(output);
  }
}
