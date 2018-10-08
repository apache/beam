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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core component to handle through a SQL statement, from explain execution plan, to generate a
 * Beam pipeline, and more.
 */
class BeamSqlEngine {
  private static final Logger LOG = LoggerFactory.getLogger(BeamSqlEngine.class);

  Planner plannerImpl;

  private BeamSqlEngine(Planner plannerImpl) {
    this.plannerImpl = plannerImpl;
  }

  public static BeamSqlEngine create(Planner plannerImpl) {
    return new BeamSqlEngine(plannerImpl);
  }

  /** Parse input SQL query, and return a {@link SqlNode} as grammar tree. */
  public SqlNode parse(String sqlStatement) throws SqlParseException {
    SqlNode parsed;
    try {
      parsed = plannerImpl.parse(sqlStatement);
    } finally {
      plannerImpl.close();
    }
    return parsed;
  }

  /** It parses and validate the input query, then convert into a {@link BeamRelNode} tree. */
  public BeamRelNode convertToBeamRel(String sqlStatement)
      throws ValidationException, RelConversionException, SqlParseException, CannotPlanException {
    BeamRelNode beamRelNode;
    try {
      SqlNode parsed = plannerImpl.parse(sqlStatement);
      SqlNode validated = plannerImpl.validate(parsed);
      LOG.info("SQL:\n" + validated);

      // root of original logical plan
      RelRoot root = plannerImpl.rel(validated);
      LOG.info("SQLPlan>\n" + RelOptUtil.toString(root.rel));

      RelTraitSet desiredTraits =
          root.rel
              .getTraitSet()
              .replace(BeamLogicalConvention.INSTANCE)
              .replace(root.collation)
              .simplify();

      // beam physical plan
      beamRelNode = (BeamRelNode) plannerImpl.transform(0, desiredTraits, root.rel);
      LOG.info("BEAMPlan>\n" + RelOptUtil.toString(beamRelNode));
    } finally {
      plannerImpl.close();
    }
    return beamRelNode;
  }
}
