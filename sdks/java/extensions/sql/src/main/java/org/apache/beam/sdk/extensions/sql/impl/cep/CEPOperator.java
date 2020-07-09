package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;

import java.io.Serializable;

public class CEPOperator implements Serializable {
    private final CEPKind cepKind;

    private CEPOperator(CEPKind cepKind) {
        this.cepKind = cepKind;
    }

    public CEPKind getCepKind() {
        return cepKind;
    }

    public static CEPOperator of(SqlOperator op) {
        switch(op.getKind()) {
            case LAST:
                return new CEPOperator(CEPKind.LAST);
            case PREV:
                return new CEPOperator(CEPKind.PREV);
            case NEXT:
                return new CEPOperator(CEPKind.NEXT);
            case EQUALS:
                return new CEPOperator(CEPKind.EQUALS);
            case GREATER_THAN:
                return new CEPOperator(CEPKind.GREATER_THAN);
            case GREATER_THAN_OR_EQUAL:
                return new CEPOperator(CEPKind.GREATER_THAN_OR_EQUAL);
            case LESS_THAN:
                return new CEPOperator(CEPKind.LESS_THAN);
            case LESS_THAN_OR_EQUAL:
                return new CEPOperator(CEPKind.LESS_THAN_OR_EQUAL);
            default:
                return new CEPOperator(CEPKind.NONE);
        }
    }
}
