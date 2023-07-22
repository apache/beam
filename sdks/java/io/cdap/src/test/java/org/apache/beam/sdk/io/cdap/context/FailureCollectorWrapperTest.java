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
package org.apache.beam.sdk.io.cdap.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link FailureCollectorWrapper}. */
@RunWith(JUnit4.class)
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
    String errorMessage = "An error has occurred";
    String expectedMessage = "Errors were encountered during validation. An error has occurred";

    FailureCollectorWrapper emptyFailureCollectorWrapper = new FailureCollectorWrapper();

    RuntimeException error = new RuntimeException(errorMessage);
    failureCollectorWrapper.addFailure(error.getMessage(), null);

    /** act && assert */
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> failureCollectorWrapper.getOrThrowException());
    assertEquals(expectedMessage, e.getMessage());

    // A case when return ValidationException with empty collector
    ArrayList<ValidationFailure> exceptionCollector =
        emptyFailureCollectorWrapper.getValidationFailures();
    assertEquals(0, exceptionCollector.size());
  }

  @Test
  public void getValidationFailures() {
    /** arrange */
    FailureCollectorWrapper failureCollectorWrapper = new FailureCollectorWrapper();
    String errorMessage = "An error has occurred";

    FailureCollectorWrapper emptyFailureCollectorWrapper = new FailureCollectorWrapper();

    RuntimeException error = new RuntimeException(errorMessage);
    failureCollectorWrapper.addFailure(error.getMessage(), null);

    /** act */
    ArrayList<ValidationFailure> exceptionCollector =
        failureCollectorWrapper.getValidationFailures();
    ArrayList<ValidationFailure> emptyExceptionCollector =
        emptyFailureCollectorWrapper.getValidationFailures();

    /** assert */
    assertEquals(1, exceptionCollector.size());
    assertEquals(errorMessage, exceptionCollector.get(0).getMessage());
    assertEquals(0, emptyExceptionCollector.size());
  }

  /**
   * Should skip schema field validation errors because they are CDAP oriented and don't affect
   * anything in our case.
   */
  @Test
  public void shouldNotThrowForSchemaFieldValidation() {
    /** arrange */
    FailureCollectorWrapper failureCollectorWrapper = new FailureCollectorWrapper();

    /** act */
    RuntimeException error = new RuntimeException("An error with cause has occurred");
    failureCollectorWrapper.addFailure(error.getMessage(), null);
    ValidationFailure failure = failureCollectorWrapper.getValidationFailures().get(0);
    ValidationFailure.Cause cause = new ValidationFailure.Cause();
    cause.addAttribute(
        CauseAttributes.INPUT_SCHEMA_FIELD, BatchContextImpl.DEFAULT_SCHEMA_FIELD_NAME);
    failure.getCauses().add(cause);

    /** assert */
    failureCollectorWrapper.getOrThrowException();
    assertEquals(0, failureCollectorWrapper.getValidationFailures().size());
  }
}
