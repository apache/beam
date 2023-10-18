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
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Fn that tags each table row with a unique id and destination table. To avoid calling
 * UUID.randomUUID() for each element, which can be costly, a randomUUID is generated only once per
 * bucket of data. The actual unique id is created by concatenating this randomUUID with a
 * sequential number.
 */
@VisibleForTesting
class TagWithUniqueIds<KeyT, ElementT>
    extends DoFn<KV<KeyT, ElementT>, KV<KeyT, TableRowInfo<ElementT>>> {
  private transient @Nullable String randomUUID = null;
  private transient long sequenceNo = 0L;

  private final @Nullable SerializableFunction<ElementT, String> elementToId;

  public TagWithUniqueIds() {
    elementToId = null;
  }

  public TagWithUniqueIds(@Nullable SerializableFunction<ElementT, String> elementToId) {
    this.elementToId = elementToId;
  }

  @StartBundle
  public void startBundle() {
    if (elementToId == null) {
      randomUUID = UUID.randomUUID().toString();
    }
  }

  /** Tag the input with a unique id. */
  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    String uniqueId;
    if (elementToId == null) {
      uniqueId = randomUUID + sequenceNo++;
    } else {
      uniqueId = elementToId.apply(context.element().getValue());
    }
    // We output on keys 0-50 to ensure that there's enough batching for
    // BigQuery.
    context.output(
        KV.of(
            context.element().getKey(),
            new TableRowInfo<>(context.element().getValue(), uniqueId)));
  }
}
