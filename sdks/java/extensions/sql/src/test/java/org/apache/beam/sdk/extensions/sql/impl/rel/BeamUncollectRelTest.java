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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@code BeamUncollectRel}. */
public class BeamUncollectRelTest extends BaseRelTest {
  private NodeStats getEstimateOf(String sql) {
    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamUncollectRel)) {
      root = root.getInput(0);
    }

    return BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());
  }

  @Test
  public void testNodeStats() {
    NodeStats estimate =
        getEstimateOf(
            "SELECT * FROM UNNEST (SELECT * FROM (VALUES (ARRAY ['a', 'b', 'c']),(ARRAY ['a', 'b', 'c']))) t1");

    Assert.assertEquals(4d, estimate.getRowCount(), 0.001);
    Assert.assertEquals(4d, estimate.getWindow(), 0.001);
    Assert.assertEquals(0., estimate.getRate(), 0.001);
  }
}
