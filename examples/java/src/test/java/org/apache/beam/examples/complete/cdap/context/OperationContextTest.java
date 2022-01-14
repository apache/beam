package org.apache.beam.examples.complete.cdap.context;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.ValidationException;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.*;

/**
 * Test class for {@link OperationContext}.
 */
public class OperationContextTest {

    @Test
    public void getLogicalStartTime() {
        /** arrange */
        Timestamp expectedStartTime = new Timestamp(System.currentTimeMillis());
        OperationContext context = new OperationContext();

        /** act */
        long actualStartTime = context.getLogicalStartTime();

        /** assert */
        // Using a range of 100 milliseconds to check the correct work of the method
        assertTrue((expectedStartTime.getTime() - actualStartTime) <= 100);
    }

    @Test
    public void getFailureCollector() {
        /** arrange */
        OperationContext context = new OperationContext();

        /** act */
        FailureCollector failureCollector = context.getFailureCollector();

        /** assert */
        ValidationException validationException = failureCollector.getOrThrowException();
        assertEquals(0, validationException.getFailures().size());
    }
}
