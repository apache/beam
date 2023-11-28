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
package org.apache.beam.it.neo4j.conditions;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.neo4j.Neo4jResourceManager;

@AutoValue
public abstract class Neo4jQueryCheck extends ConditionCheck {

  abstract Neo4jResourceManager resourceManager();

  abstract List<Map<String, Object>> expectedResult();

  abstract String query();

  @Nullable
  abstract Map<String, Object> parameters();

  @Override
  public String getDescription() {
    return String.format(
        "Neo4j check if query %s matches expected result %s", query(), expectedResult());
  }

  @Override
  @SuppressWarnings("nullness")
  protected CheckResult check() {
    List<Map<String, Object>> actualResult;
    if (parameters() != null) {
      actualResult = resourceManager().run(query(), parameters());
    } else {
      actualResult = resourceManager().run(query());
    }
    List<Map<String, Object>> expectedResult = expectedResult();
    if (actualResult == null) {
      return new CheckResult(expectedResult == null);
    }

    Set<Map<String, Object>> sortedActualResult = sort(actualResult);
    Set<Map<String, Object>> sortedExpectedResult = sort(expectedResult);

    return new CheckResult(
        sortedActualResult.equals(sortedExpectedResult),
        String.format("Expected %s to equal %s", sortedActualResult, sortedExpectedResult));
  }

  private static Set<Map<String, Object>> sort(List<Map<String, Object>> list) {
    return list.stream().map(TreeMap::new).collect(Collectors.toSet());
  }

  public static Builder builder(Neo4jResourceManager resourceManager) {
    return new AutoValue_Neo4jQueryCheck.Builder().setResourceManager(resourceManager);
  }

  /** Builder for {@link Neo4jQueryCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(Neo4jResourceManager resourceManager);

    public abstract Builder setQuery(String query);

    public abstract Builder setParameters(Map<String, Object> parameters);

    public abstract Builder setExpectedResult(List<Map<String, Object>> result);

    abstract Neo4jQueryCheck autoBuild();

    public Neo4jQueryCheck build() {
      return autoBuild();
    }
  }
}
