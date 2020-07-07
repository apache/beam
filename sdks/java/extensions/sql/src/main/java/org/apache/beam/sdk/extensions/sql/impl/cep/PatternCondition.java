package org.apache.beam.sdk.extensions.sql.impl.cep;

import org.apache.beam.sdk.values.Row;

import java.io.Serializable;

public abstract class PatternCondition implements Serializable {

    private String patternVar;

    PatternCondition(CEPPattern pattern) {
        this.patternVar = pattern.toString();
    };

    public abstract boolean eval(Row eleRow);
}
