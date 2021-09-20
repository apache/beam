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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import java.util.Objects;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCostFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * <code>VolcanoCost</code> represents the cost of a plan node.
 *
 * <p>This class is immutable: none of the methods modify any member variables.
 */
public class BeamCostModel implements RelOptCost {
  private static final double RATE_IMPORTANCE = 3600;

  static final BeamCostModel INFINITY =
      new BeamCostModel(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY) {
        @Override
        public String toString() {
          return "{inf}";
        }
      };

  static final BeamCostModel HUGE =
      new BeamCostModel(Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override
        public String toString() {
          return "{huge}";
        }
      };

  static final BeamCostModel ZERO =
      new BeamCostModel(0.0, 0.0) {
        @Override
        public String toString() {
          return "{0}";
        }
      };

  static final BeamCostModel TINY =
      new BeamCostModel(1.0, 0.001) {
        @Override
        public String toString() {
          return "{tiny}";
        }
      };

  public static final BeamCostModel.Factory FACTORY = new BeamCostModel.Factory();

  final double cpu;
  final double cpuRate;

  BeamCostModel(double cpu, double cpuRate) {
    this.cpu = Math.max(cpu, 0);
    this.cpuRate = Math.max(cpuRate, 0);
  }

  @Override
  public double getCpu() {
    return cpu;
  }

  @Override
  public boolean isInfinite() {
    return (this.equals(INFINITY))
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.cpuRate == Double.POSITIVE_INFINITY);
  }

  @Override
  public double getIo() {
    return 0;
  }

  public double getCpuRate() {
    return cpuRate;
  }

  @Override
  public boolean isLe(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    // This if is to make sure Infinity.isLe(Huge) wont be true.
    // Without this both of the costCombinations are infinity and therefore, this will return true.

    // if one of them is infinite then the only thing that matters is "that" being infinite.
    if (this.isInfinite() || that.isInfinite()) {
      return that.isInfinite();
    }

    return getCostCombination(this) <= getCostCombination(that);
  }

  @Override
  public boolean isLt(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    // This is to make sure Huge.isLt(Infinity) returns true
    if (that.isInfinite() || this.isInfinite()) {
      return !this.isInfinite();
    }

    return getCostCombination(this) < getCostCombination(that);
  }

  private static double getCostCombination(BeamCostModel cost) {
    return cost.cpu + cost.cpuRate * RATE_IMPORTANCE;
  }

  @Override
  public double getRows() {
    return 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(cpu, cpuRate);
  }

  @SuppressWarnings("NonOverridingEquals")
  @Override
  public boolean equals(RelOptCost other) {
    return other instanceof BeamCostModel
        && (this.cpu == ((BeamCostModel) other).cpu)
        && (this.cpuRate == ((BeamCostModel) other).cpuRate);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof BeamCostModel) {
      return equals((BeamCostModel) obj);
    }
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof BeamCostModel)) {
      return false;
    }
    BeamCostModel that = (BeamCostModel) other;
    return ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpuRate - that.cpuRate) < RelOptUtil.EPSILON));
  }

  @Override
  public BeamCostModel minus(RelOptCost other) {
    if (this.equals(INFINITY)) {
      return this;
    }
    BeamCostModel that = (BeamCostModel) other;
    return new BeamCostModel(this.cpu - that.cpu, this.cpuRate - that.cpuRate);
  }

  @Override
  public BeamCostModel multiplyBy(double factor) {
    if (this.equals(INFINITY)) {
      return this;
    }
    return new BeamCostModel(cpu * factor, cpuRate * factor);
  }

  @Override
  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite. (Except the window size)
    BeamCostModel that = (BeamCostModel) cost;

    if ((getCostCombination(this) != 0)
        && !Double.isInfinite(getCostCombination(this))
        && (getCostCombination(that) != 0)
        && !Double.isInfinite(getCostCombination(that))) {
      return getCostCombination(this) / getCostCombination(that);
    }

    return 1.0;
  }

  @Override
  public BeamCostModel plus(RelOptCost other) {
    BeamCostModel that = (BeamCostModel) other;
    if (this.equals(INFINITY) || that.equals(INFINITY)) {
      return INFINITY;
    }
    return new BeamCostModel(this.cpu + that.cpu, this.cpuRate + that.cpuRate);
  }

  @Override
  public String toString() {
    return "{" + cpu + " cpu, " + cpuRate + " cpuRate " + "}";
  }

  public static BeamCostModel convertRelOptCost(RelOptCost ic) {
    BeamCostModel inputCost;
    if (ic instanceof BeamCostModel) {
      inputCost = ((BeamCostModel) ic);
    } else {
      inputCost = BeamCostModel.FACTORY.makeCost(ic.getRows(), ic.getCpu(), ic.getIo());
    }
    return inputCost;
  }

  /**
   * Implementation of {@link
   * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCostFactory} that creates
   * {@link BeamCostModel}s.
   */
  public static class Factory implements RelOptCostFactory {

    @Override
    public BeamCostModel makeCost(double dRows, double dCpu, double dIo) {
      return BeamCostModel.INFINITY;
    }

    public BeamCostModel makeCost(double dCpu, double dCpuRate) {
      return new BeamCostModel(dCpu, dCpuRate);
    }

    @Override
    public BeamCostModel makeHugeCost() {
      return BeamCostModel.HUGE;
    }

    @Override
    public BeamCostModel makeInfiniteCost() {
      return BeamCostModel.INFINITY;
    }

    @Override
    public BeamCostModel makeTinyCost() {
      return BeamCostModel.TINY;
    }

    @Override
    public BeamCostModel makeZeroCost() {
      return BeamCostModel.ZERO;
    }
  }
}
