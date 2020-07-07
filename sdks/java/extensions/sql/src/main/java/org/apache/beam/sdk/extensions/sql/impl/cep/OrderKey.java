package org.apache.beam.sdk.extensions.sql.impl.cep;

import java.io.Serializable;

public class OrderKey implements Serializable {

    private final int fIndex;
    private final boolean dir;

    public OrderKey(int fIndex, boolean dir) {
        this.fIndex = fIndex;
        this.dir = dir;
    }

    public int getIndex() {
        return fIndex;
    }

    public boolean getDir() {
        return dir;
    }
}
