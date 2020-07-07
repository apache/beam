package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;

public enum Quantifier implements Serializable {
    NONE(""),
    PLUS("+"),
    ASTERISK("*"),
    QMARK("?");

    private final String repr;

    Quantifier(String repr) {
        this.repr = repr;
    }

    @Override
    public String toString() {
        return repr;
    }
}
