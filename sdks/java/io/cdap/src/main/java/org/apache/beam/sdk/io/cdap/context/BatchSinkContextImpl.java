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
package org.apache.beam.sdk.io.cdap.context;

import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;

/** Class for creating context object of different CDAP classes with batch sink type. */
public class BatchSinkContextImpl extends BatchContextImpl implements BatchSinkContext {

  /** Overrides the output configuration of this Batch job to the specified {@link Output}. */
  @Override
  public void addOutput(Output output) {
    this.outputFormatProvider =
        ((Output.OutputFormatProviderOutput) output).getOutputFormatProvider();
  }

  @Override
  public boolean isPreviewEnabled() {
    return false;
  }
}
