package org.apache.beam.sdk.extensions.sql.impl;

import com.google.auto.value.AutoValue;
import java.lang.reflect.Type;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.FunctionParameter;

@AutoValue
@Internal
public abstract class FunctionParameterBuilder {
  abstract int ordinal();
  abstract Type type();
  abstract boolean optional();

  public static Builder newBuilder() {
    return new AutoValue_FunctionParameterBuilder.Builder().setOptional(false);
  }

  @AutoValue.Builder
  public static abstract class Builder {
    public abstract Builder setOrdinal(int ordinal);
    public abstract Builder setType(Type type);
    public abstract Builder setOptional(boolean optional);

    abstract FunctionParameterBuilder autoBuild();

    public FunctionParameter build() {
      FunctionParameterBuilder autoBuilt = autoBuild();
      return new FunctionParameter() {
        @Override
        public int getOrdinal() {
          return autoBuilt.ordinal();
        }

        @Override
        public String getName() {
          // not used as Beam SQL uses its own execution engine
          return "UNUSED_FUNCTION_PARAMETER_BUILDER_DEFAULT_NAME";
        }

        @Override
        public RelDataType getType(RelDataTypeFactory typeFactory) {
          return CalciteUtils.sqlTypeWithAutoCast(typeFactory, autoBuilt.type());
        }

        @Override
        public boolean isOptional() {
          return autoBuilt.optional();
        }
      };
    }
  }
}
