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

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for reading and writing to BigQuery
 * with {@link BigQueryIO}.
 *
 * <p><b>Internal only:</b> This is actively being worked on and will likely change. We provide no
 * backwards compatibility guarantees, and it should not be implemented outside the Beam repository.
 */
@Internal
@Experimental
public class BigQuerySchemaIOProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaIOConfiguration> {

  public BigQuerySchemaIOProvider() {
    super();
  }

  /** Returns the expected class of the configuration object. */
  @Override
  public Class<BigQuerySchemaIOConfiguration> configurationClass() {
    return BigQuerySchemaIOConfiguration.class;
  }

  /**
   * Produces a {@link SchemaTransform} implementation based on the {@link
   * BigQuerySchemaIOConfiguration} details.
   */
  @Override
  public SchemaTransform from(BigQuerySchemaIOConfiguration configuration) {
    return BigQuerySchemaTransform.of(configuration);
  }

  /** Returns an id that uniquely identifies this transform. */
  @Override
  public String identifier() {
    return BigQuerySchemaIOConfiguration.IDENTIFIER;
  }

  /** Returns the input collection names of this transform. */
  @Override
  public List<String> inputCollectionNames() {
    // TODO: determine valid input collection names for JobType
    return Collections.emptyList();
  }

  /** Returns the output collection names of this transform. */
  @Override
  public List<String> outputCollectionNames() {
    // TODO: determine valid output collection names for JobType
    return Collections.emptyList();
  }
}
