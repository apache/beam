package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.sdk.extensions.sql.impl.SqlConversionException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;

import java.io.Serializable;

public abstract class CEPOperation implements Serializable {

    public static CEPOperation of(RexNode operation) {
        if(operation.getClass() == RexCall.class) {
            RexCall call = (RexCall) operation;
            return CEPCall.of(call);
        } else if(operation.getClass() == RexLiteral.class) {
            RexLiteral lit = (RexLiteral) operation;
            return CEPLiteral.of(lit);
        } else if(operation.getClass() == RexPatternFieldRef.class) {
            RexPatternFieldRef fieldRef = (RexPatternFieldRef) operation;
            return CEPFieldRef.of(fieldRef);
        } else {
            throw new SqlConversionException("RexNode type not supported");
        }
    }
}
