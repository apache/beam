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
package org.apache.beam.sdk.extensions.sql.zetasql;

import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.Context;
import org.codehaus.commons.nullanalysis.Nullable;

/**
 * Calcite parser context to pass the configuration down to planner and rules so that we can
 * configure custom name resolution.
 */
class TableResolutionContext implements Context {

  /** Table resolvers, associating top-level schema to a custom name resolution logic. */
  private final Map<String, TableResolver> resolvers;

  /** Assigns a custom table resolver to the given schema. */
  static TableResolutionContext of(String topLevelSchema, TableResolver resolver) {
    return new TableResolutionContext(ImmutableMap.of(topLevelSchema, resolver));
  }

  /**
   * Uses the resolution logic that joins the table path into a single compound identifier and then
   * queries the schema once, instead of drilling down into subschemas.
   */
  static TableResolutionContext joinCompoundIds(String topLevelSchema) {
    return of(topLevelSchema, TableResolver.JOIN_INTO_COMPOUND_ID);
  }

  TableResolutionContext with(String topLevelSchema, TableResolver resolver) {
    return new TableResolutionContext(
        ImmutableMap.<String, TableResolver>builder()
            .putAll(this.resolvers)
            .put(topLevelSchema, resolver)
            .build());
  }

  boolean hasCustomResolutionFor(String schemaName) {
    return resolvers.containsKey(schemaName);
  }

  @Nullable
  TableResolver getTableResolver(String schemaName) {
    return resolvers.get(schemaName);
  }

  private TableResolutionContext(Map<String, TableResolver> resolvers) {
    this.resolvers = resolvers;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> c) {
    return c.isAssignableFrom(TableResolutionContext.class) ? (T) this : null;
  }
}
