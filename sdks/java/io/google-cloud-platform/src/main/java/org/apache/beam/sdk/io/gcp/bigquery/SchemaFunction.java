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
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A user-suppled schema function, mapping a {@link TableDestination} to a {@link TableSchema}.
 *
 * <p>A function can declare that it wants access to a map-valued {@link PCollectionView} and use
 * that map to assign {@link TableSchema}s to tables. This is used by
 * {@link BigQueryIO.Write#withSchemaFromView}. In that case the map is assumed to map string
 * tablespecs to json-formatted schemas.
 */
public abstract class SchemaFunction
    implements SerializableFunction<TableDestination, TableSchema> {
  private PCollectionView<Map<String, String>> sideInput;
  private Map<String, String> materialized;

  public SchemaFunction() {
  }

  public SchemaFunction withSideInput(PCollectionView<Map<String, String>> sideInput) {
    this.sideInput = sideInput;
    return this;
  }

  public PCollectionView<Map<String, String>> getSideInput() {
    return sideInput;
  }

  public Map<String, String> getSideInputValue() {
    return materialized;
  }

  void setSideInputValue(Map<String, String> value) {
    materialized = value;
  }
}
