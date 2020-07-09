package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;

public enum CEPKind implements Serializable {
    LAST,
    PREV,
    NEXT,
    EQUALS,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    NONE
}
