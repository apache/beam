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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelCollation;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelDistribution;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Statistic;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.ImmutableBitSet;

/** This class stores row count statistics. */
@Experimental
@Internal
public class BeamTableStatistics implements Serializable, Statistic {
  public static final BeamTableStatistics BOUNDED_UNKNOWN = new BeamTableStatistics(100d, 0d, true);
  public static final BeamTableStatistics UNBOUNDED_UNKNOWN =
      new BeamTableStatistics(0d, 0.1, true);
  private final boolean unknown;
  private final Double rowCount;
  private final Double rate;

  private BeamTableStatistics(Double rowCount, Double rate, boolean isUnknown) {
    this.rowCount = rowCount;
    this.rate = rate;
    this.unknown = isUnknown;
  }

  private BeamTableStatistics(Double rowCount, Double rate) {
    this(rowCount, rate, false);
  }

  public static BeamTableStatistics createBoundedTableStatistics(Double rowCount) {
    return new BeamTableStatistics(rowCount, 0d);
  }

  public static BeamTableStatistics createUnboundedTableStatistics(Double rate) {
    return new BeamTableStatistics(0d, rate);
  }

  public Double getRate() {
    return rate;
  }

  public boolean isUnknown() {
    return unknown;
  }

  @Override
  public Double getRowCount() {
    return rowCount;
  }

  @Override
  public boolean isKey(ImmutableBitSet columns) {
    return false;
  }

  @Override
  public List<ImmutableBitSet> getKeys() {
    return Collections.emptyList(); // FIXME BEAM-9379: Is this correct???
  }

  @Override
  public List<RelReferentialConstraint> getReferentialConstraints() {
    return ImmutableList.of();
  }

  @Override
  public List<RelCollation> getCollations() {
    return ImmutableList.of();
  }

  @Override
  public RelDistribution getDistribution() {
    return RelDistributionTraitDef.INSTANCE.getDefault();
  }
}
