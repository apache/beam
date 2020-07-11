package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;

public class OrderKey implements Serializable {

    private final int fIndex;
    private final boolean inv;

    public OrderKey(int fIndex, boolean inv) {
        this.fIndex = fIndex;
        this.inv = inv;
    }

    public int getIndex() {
        return fIndex;
    }

    public boolean getDir() {
        return inv;
    }
}
