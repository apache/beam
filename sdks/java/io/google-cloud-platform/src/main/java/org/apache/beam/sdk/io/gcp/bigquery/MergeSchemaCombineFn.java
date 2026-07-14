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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.util.Iterator;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;

/** A CombineFn to merge TableSchemas. */
public class MergeSchemaCombineFn extends Combine.CombineFn<TableSchema, TableSchema, TableSchema> {
  @Override
  public TableSchema createAccumulator() {
    return TableSchema.newBuilder().build();
  }

  @Override
  public TableSchema addInput(TableSchema accumulator, TableSchema input) {
    if (input.equals(accumulator)) {
      return accumulator;
    }
    return UpgradeTableSchema.mergeSchemas(accumulator, input);
  }

  @Override
  public Coder<TableSchema> getAccumulatorCoder(
      CoderRegistry registry, Coder<TableSchema> inputCoder) {
    return inputCoder;
  }

  @Override
  public Coder<TableSchema> getDefaultOutputCoder(
      CoderRegistry registry, Coder<TableSchema> inputCoder) throws CannotProvideCoderException {
    return inputCoder;
  }

  @Override
  public TableSchema mergeAccumulators(Iterable<TableSchema> accumulators) {
    checkNotNull(accumulators, "accumulators must be non-null");

    Iterator<TableSchema> iter = accumulators.iterator();
    if (!iter.hasNext()) {
      return createAccumulator();
    }

    TableSchema merged = iter.next();
    while (iter.hasNext()) {
      merged = addInput(merged, iter.next());
    }
    return merged;
  }

  @Override
  public TableSchema extractOutput(TableSchema accumulator) {
    return accumulator;
  }
}
