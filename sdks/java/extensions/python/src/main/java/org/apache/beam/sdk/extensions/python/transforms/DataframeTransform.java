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
package org.apache.beam.sdk.extensions.python.transforms;

import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Wrapper for invoking external Python {@code DataframeTransform}. */
public class DataframeTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private final String func;
  private final boolean includeIndexes;
  private final String expansionService;

  /**
   * Instantiates a multi-language wrapper for a Python DataframeTransform with a given lambda
   * function.
   *
   * @param func A Python lambda function that accepts Pandas dataframe object.
   * @return A {@link DataframeTransform} for the given lambda function.
   */
  public static DataframeTransform of(String func) {
    return new DataframeTransform(func, false, "");
  }

  /**
   * Sets include_indexes option for DataframeTransform.
   *
   * @return A {@link DataframeTransform} with include_indexes option enabled.
   */
  public DataframeTransform withIndexes() {
    return new DataframeTransform(func, true, expansionService);
  }

  /**
   * Sets an expansion service endpoint for DataframeTransform.
   *
   * @param expansionService A URL for a Python expansion service.
   * @return A {@link DataframeTransform} for the given expansion service endpoint.
   */
  public DataframeTransform withExpansionService(String expansionService) {
    return new DataframeTransform(func, includeIndexes, expansionService);
  }

  private DataframeTransform(String func, boolean includeIndexes, String expansionService) {
    this.func = func;
    this.includeIndexes = includeIndexes;
    this.expansionService = expansionService;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input.apply(
        PythonExternalTransform.<PCollection<Row>, PCollection<Row>>from(
                "apache_beam.dataframe.transforms.DataframeTransform", expansionService)
            .withKwarg("func", PythonCallableSource.of(func))
            .withKwarg("include_indexes", includeIndexes));
  }
}
