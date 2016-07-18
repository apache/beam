package org.apache.beam.sdk.io.kinesis.client;

import com.amazonaws.AmazonServiceException;

/**
 * Created by p.pastuszka on 21.06.2016.
 */
public class TransientKinesisException extends Exception {
    public TransientKinesisException(AmazonServiceException e) {
        super(e);
    }
}
