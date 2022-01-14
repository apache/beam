/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples.complete.cdap.context;

import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test class for {@link FailureCollectorWrapper}.
 */
public class FailureCollectorWrapperTest {

    @Test
    public void addFailure() {
        /** arrange */
        FailureCollectorWrapper failureCollectorWrapper = new FailureCollectorWrapper();

        /** act */
        RuntimeException error = new RuntimeException("An error has occurred");
        failureCollectorWrapper.addFailure(error.getMessage(), null);

        /** assert */
        assertThrows(ValidationException.class, () -> failureCollectorWrapper.getOrThrowException());
    }

    @Test
    public void getOrThrowException() {
        /** arrange */
        FailureCollectorWrapper failureCollectorWrapper = new FailureCollectorWrapper();
        String message = "An error has occurred";
        String expectedMessage = "Errors were encountered during validation. An error has occurred";

        FailureCollectorWrapper emptyFailureCollectorWrapper = new FailureCollectorWrapper();

        RuntimeException error = new RuntimeException(message);
        failureCollectorWrapper.addFailure(error.getMessage(), null);

        /** act && assert */
        ValidationException e = assertThrows(ValidationException.class, () -> failureCollectorWrapper.getOrThrowException());
        assertEquals(expectedMessage, e.getMessage());

        // A case when return ValidationException with empty collector
        ArrayList<ValidationFailure> exceptionCollector = emptyFailureCollectorWrapper.getValidationFailures();
        assertEquals(0, exceptionCollector.size());
    }

    @Test
    public void getValidationFailures() {
        /** arrange */
        FailureCollectorWrapper failureCollectorWrapper = new FailureCollectorWrapper();
        String message = "An error has occurred";

        FailureCollectorWrapper emptyFailureCollectorWrapper = new FailureCollectorWrapper();

        RuntimeException error = new RuntimeException(message);
        failureCollectorWrapper.addFailure(error.getMessage(), null);

        /** act */
        ArrayList<ValidationFailure> exceptionCollector = failureCollectorWrapper.getValidationFailures();
        ArrayList<ValidationFailure> emptyExceptionCollector = emptyFailureCollectorWrapper.getValidationFailures();


        /** assert */
        assertEquals(1, exceptionCollector.size());
        String actualMessage = exceptionCollector.get(0).getMessage();
        assertEquals(message, actualMessage);

        assertEquals(0, emptyExceptionCollector.size());
    }
}
