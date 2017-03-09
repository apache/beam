package org.beam.sdk.java.sql.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.ebay.dss.tora.beam_sql_poc.planner.BeamSqlUnsupportedException;
import com.google.common.base.Joiner;

public class CalciteToSpEL {

  public static String rexcall2SpEL(RexCall cdn, RecordType recordType) {
    List<String> parts = new ArrayList<>();
    for (RexNode subcdn : cdn.operands) {
      if (subcdn instanceof RexCall) {
        parts.add(rexcall2SpEL((RexCall) subcdn, recordType));
      } else {
        parts.add(subcdn instanceof RexInputRef
            ? "#map.getFieldValue('"
                + recordType.getFieldsName().get(((RexInputRef) subcdn).getIndex()) + "')"
            : subcdn.toString());
      }
    }

    String opName = cdn.op.getName();
    switch (cdn.op.getClass().getSimpleName()) {
    case "SqlMonotonicBinaryOperator": // +-*
    case "SqlBinaryOperator": // > < = >= <= <> OR AND || / .
      switch (cdn.op.getName().toUpperCase()) {
      case "AND":
        return String.format(" ( %s ) ", Joiner.on("&&").join(parts) );
      case "OR":
        return String.format(" ( %s ) ", Joiner.on("||").join(parts) );
      case "=":
        return String.format(" ( %s ) ", Joiner.on("==").join(parts) );
      case "<>":
        return String.format(" ( %s ) ", Joiner.on("!=").join(parts) );
      default:
        return String.format(" ( %s ) ", Joiner.on(cdn.op.getName().toUpperCase()).join(parts) );
      }
    case "SqlCaseOperator": // CASE
      return String.format(" (%s ? %s : %s)", parts.get(0), parts.get(1), parts.get(2));
    case "SqlCastFunction": // CAST
      return parts.get(0);
    case "SqlPostfixOperator":
      switch (opName.toUpperCase()) {
      case "IS NULL":
        return String.format(" null == %s ", parts.get(0));
      case "IS NOT NULL":
        return String.format(" null != %s ", parts.get(0));
      default:
        throw new BeamSqlUnsupportedException();
      }
    case "SqlFloorFunction":
      return String.format("%s / %s", parts.get(0), 3600000);
//      throw new BeamSqlUnsupportedException("TODO");
    default:
      throw new BeamSqlUnsupportedException();
    }
  }

}
