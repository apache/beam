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

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Compute the mapping of destinations to json-formatted schema objects. */
class CalculateSchemas<DestinationT>
    extends PTransform<
        PCollection<KV<DestinationT, TableRow>>, PCollectionView<Map<DestinationT, String>>> {
  private static final Logger LOG = LoggerFactory.getLogger(CalculateSchemas.class);

  private final DynamicDestinations<?, DestinationT> dynamicDestinations;

  public CalculateSchemas(DynamicDestinations<?, DestinationT> dynamicDestinations) {
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollectionView<Map<DestinationT, String>> expand(
      PCollection<KV<DestinationT, TableRow>> input) {
    List<PCollectionView<?>> sideInputs = Lists.newArrayList();
    sideInputs.addAll(dynamicDestinations.getSideInputs());

    return input
        .apply("Keys", Keys.<DestinationT>create())
        .apply("Distinct Keys", Distinct.<DestinationT>create())
        .apply(
            "GetSchemas",
            ParDo.of(
                    new DoFn<DestinationT, KV<DestinationT, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        dynamicDestinations.setSideInputAccessorFromProcessContext(c);
                        TableSchema tableSchema = dynamicDestinations.getSchema(c.element());
                        if (tableSchema != null) {
                          // If the createDisposition is CREATE_NEVER, then there's no need for a
                          // schema, and getSchema might return null. In this case, we simply
                          // leave it out of the map.
                          c.output(KV.of(c.element(), BigQueryHelpers.toJsonString(tableSchema)));
                        }
                      }
                    })
                .withSideInputs(sideInputs))
        .apply("asMap", View.<DestinationT, String>asMap());
  }
}
