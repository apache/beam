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
package org.apache.beam.sdk.extensions.sql.zetasql.unnest;

import java.util.List;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelInput;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelWriter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.MapSqlType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;

/**
 * This class is a copy of Uncollect.java in Calcite:
 * https://github.com/apache/calcite/blob/calcite-1.20.0/core/src/main/java/org/apache/calcite/rel/core/Uncollect.java
 * except that in deriveUncollectRowType() it does not unwrap array elements of struct type.
 *
 * <p>Details of why unwrapping structs breaks ZetaSQL UNNEST syntax is in
 * https://issues.apache.org/jira/browse/BEAM-10896.
 */
public class ZetaSqlUnnest extends SingleRel {
  public final boolean withOrdinality;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates an Uncollect.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public ZetaSqlUnnest(
      RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean withOrdinality) {
    super(cluster, traitSet, input);
    this.withOrdinality = withOrdinality;
  }

  /** Creates an Uncollect by parsing serialized output. */
  public ZetaSqlUnnest(RelInput input) {
    this(
        input.getCluster(),
        input.getTraitSet(),
        input.getInput(),
        input.getBoolean("withOrdinality", false));
  }

  /**
   * Creates an Uncollect.
   *
   * <p>Each field of the input relational expression must be an array or multiset.
   *
   * @param traitSet Trait set
   * @param input Input relational expression
   * @param withOrdinality Whether output should contain an ORDINALITY column
   */
  public static ZetaSqlUnnest create(RelTraitSet traitSet, RelNode input, boolean withOrdinality) {
    final RelOptCluster cluster = input.getCluster();
    return new ZetaSqlUnnest(cluster, traitSet, input, withOrdinality);
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).itemIf("withOrdinality", withOrdinality, withOrdinality);
  }

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs));
  }

  public RelNode copy(RelTraitSet traitSet, RelNode input) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new ZetaSqlUnnest(getCluster(), traitSet, input, withOrdinality);
  }

  @Override
  protected RelDataType deriveRowType() {
    return deriveUncollectRowType(input, withOrdinality);
  }

  /**
   * Returns the row type returned by applying the 'UNNEST' operation to a relational expression.
   *
   * <p>Each column in the relational expression must be a multiset of structs or an array. The
   * return type is the type of that column, plus an ORDINALITY column if {@code withOrdinality}.
   */
  public static RelDataType deriveUncollectRowType(RelNode rel, boolean withOrdinality) {
    RelDataType inputType = rel.getRowType();
    assert inputType.isStruct() : inputType + " is not a struct";
    final List<RelDataTypeField> fields = inputType.getFieldList();
    final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();

    if (fields.size() == 1 && fields.get(0).getType().getSqlTypeName() == SqlTypeName.ANY) {
      // Component type is unknown to Uncollect, build a row type with input column name
      // and Any type.
      return builder.add(fields.get(0).getName(), SqlTypeName.ANY).nullable(true).build();
    }

    for (RelDataTypeField field : fields) {
      if (field.getType() instanceof MapSqlType) {
        builder.add(SqlUnnestOperator.MAP_KEY_COLUMN_NAME, field.getType().getKeyType());
        builder.add(SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, field.getType().getValueType());
      } else {
        RelDataType ret = field.getType().getComponentType();
        assert null != ret;
        // Only difference than Uncollect.java: treats record type and scalar type equally
        builder.add(SqlUtil.deriveAliasFromOrdinal(field.getIndex()), ret);
      }
    }
    if (withOrdinality) {
      builder.add(SqlUnnestOperator.ORDINALITY_COLUMN_NAME, SqlTypeName.INTEGER);
    }
    return builder.build();
  }
}
