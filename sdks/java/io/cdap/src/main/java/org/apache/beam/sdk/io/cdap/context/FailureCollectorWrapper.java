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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/** Class FailureCollectorWrapper is a class for collecting ValidationFailure. */
public class FailureCollectorWrapper implements FailureCollector {
  private ArrayList<ValidationFailure> failuresCollection;

  public FailureCollectorWrapper() {
    this.failuresCollection = new ArrayList<>();
  }

  @Override
  public ValidationFailure addFailure(String message, @Nullable String correctiveAction) {
    ValidationFailure validationFailure = new ValidationFailure(message, correctiveAction);
    failuresCollection.add(validationFailure);

    return validationFailure;
  }

  @Override
  public ValidationException getOrThrowException() throws ValidationException {

    // We skip schema field validation errors because they are CDAP oriented and don't affect
    // anything in our case
    List<ValidationFailure> schemaValidationFailures = new ArrayList<>();
    for (ValidationFailure failure : failuresCollection) {
      List<ValidationFailure.Cause> causes = failure.getCauses();
      if (causes != null) {
        for (ValidationFailure.Cause cause : causes) {
          String inputField = cause.getAttribute(CauseAttributes.INPUT_SCHEMA_FIELD);
          if (BatchContextImpl.DEFAULT_SCHEMA_FIELD_NAME.equals(inputField)) {
            schemaValidationFailures.add(failure);
          }
        }
      }
    }
    failuresCollection.removeAll(schemaValidationFailures);
    if (failuresCollection.isEmpty()) {
      return new ValidationException(this.failuresCollection);
    }

    throw new ValidationException(this.failuresCollection);
  }

  @Override
  public ArrayList<ValidationFailure> getValidationFailures() {
    return this.failuresCollection;
  }
}
