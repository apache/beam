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
import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.Failure;
import org.apache.beam.sdk.schemas.io.GenericDlqProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
@AutoService(GenericDlqProvider.class)
public class BigQueryDlqProvider implements GenericDlqProvider {
  @Override
  public String identifier() {
    return "bigquery";
  }

  @Override
  public PTransform<PCollection<Failure>, PDone> newDlqTransform(String config) {
    return new DlqTransform(config);
  }

  private static class DlqTransform extends PTransform<PCollection<Failure>, PDone> {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDlqProvider.class);
    private final String tableSpec;

    DlqTransform(String tableSpec) {
      this.tableSpec = tableSpec;
    }

    @Override
    public PDone expand(PCollection<Failure> input) {
      input
          .apply(
              "Failure to Row",
              MapElements.into(TypeDescriptor.of(TableRow.class)).via(DlqTransform::getTableRow))
          .apply("Write Failures to BigQuery", BigQueryIO.writeTableRows().to(tableSpec))
          .getFailedInsertsWithErr()
          .apply(
              "Log insert failures",
              MapElements.into(TypeDescriptor.of(Void.class))
                  .via(
                      x -> {
                        LOG.error("Failed to insert error into BigQuery table. {}", x);
                        return null;
                      }));
      return PDone.in(input.getPipeline());
    }

    private static TableRow getTableRow(Failure failure) {
      Row row =
          Row.withSchema(
                  Schema.builder().addByteArrayField("payload").addStringField("error").build())
              .withFieldValue("payload", failure.getPayload())
              .withFieldValue("error", failure.getError())
              .build();
      return BigQueryUtils.toTableRow(row);
    }
  }
}
