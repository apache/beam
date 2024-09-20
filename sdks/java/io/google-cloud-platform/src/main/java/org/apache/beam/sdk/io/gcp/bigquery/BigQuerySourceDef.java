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

import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Represents a source used for {@link BigQueryIO#read(SerializableFunction)}. Currently this could
 * be either a table or a query. Direct read sources are not yet supported.
 */
interface BigQuerySourceDef extends Serializable {
  /**
   * Convert this source definition into a concrete source implementation.
   *
   * @param stepUuid Job UUID
   * @param coder Coder
   * @param readerFactory Reader factory
   * @param useAvroLogicalTypes Use avro logical types i.e DATE, TIME
   * @param <T> Type of the resulting PCollection
   * @return An implementation of {@link BigQuerySourceBase}
   */
  <T> BigQuerySourceBase<T> toSource(
      String stepUuid,
      Coder<T> coder,
      SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory,
      boolean useAvroLogicalTypes);

  /**
   * Extract the {@link TableSchema} corresponding to this source.
   *
   * @param bqOptions BigQueryOptions
   * @return table schema of the source
   * @throws BigQuerySchemaRetrievalException if schema retrieval fails
   */
  TableSchema getTableSchema(BigQueryOptions bqOptions);

  /**
   * Extract the Beam {@link Schema} corresponding to this source.
   *
   * @param bqOptions BigQueryOptions
   * @return Beam schema of the source
   * @throws BigQuerySchemaRetrievalException if schema retrieval fails
   */
  default Schema getBeamSchema(BigQueryOptions bqOptions) {
    return BigQueryUtils.fromTableSchema(getTableSchema(bqOptions));
  }
}
