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
package org.apache.beam.sdk.extensions.sql.impl;

import com.google.auto.value.AutoOneOf;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RuleSet;

/**
 * An interface that planners should implement to convert sql statement to {@link BeamRelNode} or
 * {@link SqlNode}.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public interface QueryPlanner {
  /** It parses and validate the input query, then convert into a {@link BeamRelNode} tree. */
  BeamRelNode convertToBeamRel(String sqlStatement, QueryParameters queryParameters)
      throws ParseException, SqlConversionException;

  /** Parse input SQL query, and return a {@link SqlNode} as grammar tree. */
  SqlNode parse(String sqlStatement) throws ParseException;

  @AutoOneOf(QueryParameters.Kind.class)
  abstract class QueryParameters {
    public enum Kind {
      NONE,
      NAMED,
      POSITIONAL
    }

    public abstract Kind getKind();

    abstract void none();

    public abstract Map<String, ?> named();

    public abstract List<?> positional();

    public static QueryParameters ofNone() {
      return AutoOneOf_QueryPlanner_QueryParameters.none();
    }

    public static QueryParameters ofNamed(Map<String, ?> namedParams) {
      ImmutableMap.Builder builder = ImmutableMap.builder();
      for (Map.Entry<String, ?> e : namedParams.entrySet()) {
        builder.put(e.getKey().toLowerCase(), e.getValue());
      }
      return AutoOneOf_QueryPlanner_QueryParameters.named(builder.build());
    }

    public static QueryParameters ofPositional(List positionalParams) {
      return AutoOneOf_QueryPlanner_QueryParameters.positional(
          ImmutableList.copyOf(positionalParams));
    }
  }

  interface Factory {
    QueryPlanner createPlanner(JdbcConnection jdbcConnection, Collection<RuleSet> ruleSets);
  }
}
