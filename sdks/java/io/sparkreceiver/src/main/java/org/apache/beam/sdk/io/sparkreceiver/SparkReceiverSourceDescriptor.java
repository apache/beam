package org.apache.beam.sdk.io.sparkreceiver;

import java.io.Serializable;

@SuppressWarnings("UnusedVariable")
public class SparkReceiverSourceDescriptor implements Serializable {

    private String objectType;

    public SparkReceiverSourceDescriptor(String objectType) {
        this.objectType = objectType;
    }
}
