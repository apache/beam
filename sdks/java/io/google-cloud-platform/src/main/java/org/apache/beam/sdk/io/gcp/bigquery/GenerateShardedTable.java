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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;

/**
 * Given a write to a specific table, assign that to one of the {@link
 * GenerateShardedTable#numShards} keys assigned to that table.
 */
class GenerateShardedTable<ElementT>
    extends DoFn<KV<TableDestination, ElementT>, KV<ShardedKey<String>, ElementT>> {
  private final int numShards;

  GenerateShardedTable(int numShards) {
    this.numShards = numShards;
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
    ThreadLocalRandom randomGenerator = ThreadLocalRandom.current();
    // We output on keys 0-numShards.
    String tableSpec = context.element().getKey().getTableSpec();
    context.output(
        KV.of(
            ShardedKey.of(tableSpec, randomGenerator.nextInt(0, numShards)),
            context.element().getValue()));
  }
}
