package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexPatternFieldRef;

public class CEPFieldRef extends CEPOperation {
    private final int fieldIndex;

    private CEPFieldRef(int fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    public static CEPFieldRef of(RexPatternFieldRef rexFieldRef) {
        return new CEPFieldRef(rexFieldRef.getIndex());
    }

    public int getIndex() {
        return fieldIndex;
    }
}
