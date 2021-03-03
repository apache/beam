package org.apache.beam.sdk.extensions.sql.impl;

import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.AggContext;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.AggResetContext;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.AggResultContext;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.ImplementableAggFunction;

/**
 * The base class that all internal UDAFs must override to b
 */
@Internal
public abstract class AggregateCallCombineFnFactory implements ImplementableAggFunction {
  public abstract CombineFn<?, ?, ?> getCombineFn();

  @Override
  public final AggImplementor getImplementor(boolean b) {
    // not used as Beam SQL uses its own execution engine
    return new AggImplementor() {
      @Override
      public List<Type> getStateType(AggContext aggContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void implementReset(AggContext aggContext, AggResetContext aggResetContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void implementAdd(AggContext aggContext, AggAddContext aggAddContext) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Expression implementResult(AggContext aggContext, AggResultContext aggResultContext) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
