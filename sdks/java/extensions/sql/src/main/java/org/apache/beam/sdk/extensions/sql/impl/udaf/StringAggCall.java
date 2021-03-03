package org.apache.beam.sdk.extensions.sql.impl.udaf;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.AggregateCallCombineFnFactory;
import org.apache.beam.sdk.extensions.sql.impl.FunctionParameterBuilder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class StringAggCall extends AggregateCallCombineFnFactory {
  @Override
  public CombineFn<?, ?, ?> getCombineFn() {
    return new StringAgg();
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
    return relDataTypeFactory.createJavaType(Object.class);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return ImmutableList.of(
        FunctionParameterBuilder.newBuilder().setOrdinal(0).setType(Object.class).build(),
        FunctionParameterBuilder.newBuilder().setOrdinal(1).setType(Object.class).setOptional(true).build());
  }
}
