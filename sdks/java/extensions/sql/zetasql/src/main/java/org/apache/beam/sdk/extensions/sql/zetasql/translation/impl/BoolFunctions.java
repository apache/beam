package org.apache.beam.sdk.extensions.sql.zetasql.translation.impl;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.function.Strict;

public class BoolFunctions {

    @Strict
    public static Boolean logicalAnd(Boolean bool1, Boolean bool2) {
        return Boolean.logicalAnd(bool1, bool2);
    }
}
