package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

public class CEPCall extends CEPOperation {

    private final CEPOperator operator;
    private final List<CEPOperation> operands;

    private CEPCall(CEPOperator operator, List<CEPOperation> operands) {
        this.operator = operator;
        this.operands = operands;
    }

    public CEPOperator getOperator() {
        return operator;
    }

    public List<CEPOperation> getOperands() {
        return operands;
    }

    public static CEPCall of(RexCall operation) {
        SqlOperator call = operation.getOperator();
        CEPOperator myOp = CEPOperator.of(call);

        ArrayList<CEPOperation> operandsList = new ArrayList<>();
        for(RexNode i : operation.getOperands()) {
            if(i.getClass() == RexCall.class) {
                CEPCall callToAdd = CEPCall.of((RexCall) i);
                operandsList.add(callToAdd);
            } else if(i.getClass() == RexLiteral.class) {
                RexLiteral lit = (RexLiteral) i;
                CEPLiteral litToAdd = CEPLiteral.of(lit);
                operandsList.add(litToAdd);
            } else if(i.getClass() == RexPatternFieldRef.class) {
                RexPatternFieldRef fieldRef = (RexPatternFieldRef) i;
                CEPFieldRef fieldRefToAdd = CEPFieldRef.of(fieldRef);
                operandsList.add(fieldRefToAdd);
            }
        }

        return new CEPCall(myOp, operandsList);

    }

}
