package org.apache.beam.sdk.extensions.sql.zetasql.translation.impl;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.function.Strict;

public class BitwiseFunctions {

    @Strict
    public static Integer bitXOr(Integer int1, Integer int2) {
        return int1 ^ int2;
    }

    @Strict
    public static Integer bitXOr(Integer int1, Integer int2, Integer int3) {
        return int1 ^ int2 ^ int3;
    }

}

