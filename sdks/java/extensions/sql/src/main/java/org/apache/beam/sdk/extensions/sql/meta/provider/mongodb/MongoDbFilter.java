package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;

public class MongoDbFilter implements BeamSqlTableFilter {

  private List<RexNode> supported;
  private List<RexNode> unsupported;

  public MongoDbFilter(List<RexNode> predicateCNF) {
    supported = new ArrayList<>();
    unsupported = new ArrayList<>();

    for (RexNode node : predicateCNF) {
      if (!node.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        throw new RuntimeException(
            "Predicate node '"
                + node.getClass().getSimpleName()
                + "' should be a boolean expression, but was: "
                + node.getType().getSqlTypeName());
      }

      if (isSupported(node).getLeft()) {
        supported.add(node);
      } else {
        unsupported.add(node);
      }
    }
  }

  @Override
  public List<RexNode> getNotSupported() {
    return unsupported;
  }

  private Pair<Boolean, Integer> isSupported(RexNode node) {
    int numberOfInputRefs = 0;
    boolean isSupported = false;



    return Pair.of(isSupported, numberOfInputRefs);
  }
}
