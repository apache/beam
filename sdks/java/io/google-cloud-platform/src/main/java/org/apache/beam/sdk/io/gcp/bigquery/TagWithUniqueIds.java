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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToTableSpec;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Fn that tags each table row with a unique id and destination table.
 * To avoid calling UUID.randomUUID() for each element, which can be costly,
 * a randomUUID is generated only once per bucket of data. The actual unique
 * id is created by concatenating this randomUUID with a sequential number.
 */
@VisibleForTesting
class TagWithUniqueIds
    extends DoFn<KV<ShardedKey<String>, TableRow>, KV<ShardedKey<String>, TableRowInfo>> {

  private transient String randomUUID;
  private transient long sequenceNo = 0L;

  @StartBundle
  public void startBundle(Context context) {
    randomUUID = UUID.randomUUID().toString();
  }

  /** Tag the input with a unique id. */
  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
    String uniqueId = randomUUID + sequenceNo++;
    // We output on keys 0-50 to ensure that there's enough batching for
    // BigQuery.
    context.output(KV.of(context.element().getKey(),
        new TableRowInfo(context.element().getValue(), uniqueId)));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
  }
}
