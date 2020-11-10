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

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
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
   * @param parseFn Parse function
   * @param useAvroLogicalTypes Use avro logical types i.e DATE, TIME
   * @param <T> Type of the resulting PCollection
   * @return An implementation of {@link BigQuerySourceBase}
   */
  <T> BigQuerySourceBase<T> toSource(
      String stepUuid,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      boolean useAvroLogicalTypes);

  /**
   * Extract the Beam {@link Schema} corresponding to this source.
   *
   * @param bqOptions BigQueryOptions
   * @return Beam schema of the source
   * @throws BigQuerySchemaRetrievalException if schema retrieval fails
   */
  @Experimental(Kind.SCHEMAS)
  Schema getBeamSchema(BigQueryOptions bqOptions);
}
