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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy.Context;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link InsertRetryPolicy}. */
@RunWith(JUnit4.class)
public class InsertRetryPolicyTest {
  @Test
  public void testNeverRetry() {
    assertFalse(
        InsertRetryPolicy.neverRetry()
            .shouldRetry(new Context(new TableDataInsertAllResponse.InsertErrors())));
  }

  @Test
  public void testAlwaysRetry() {
    assertTrue(
        InsertRetryPolicy.alwaysRetry()
            .shouldRetry(new Context(new TableDataInsertAllResponse.InsertErrors())));
  }

  @Test
  public void testDontRetryPersistentErrors() {
    InsertRetryPolicy policy = InsertRetryPolicy.retryTransientErrors();
    assertTrue(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "unavailable"))));
    assertFalse(policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "invalid"))));
    assertFalse(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "invalidQuery"))));
    assertFalse(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "notImplemented"))));
  }

  static class RetryAllExceptInvalidQuery extends InsertRetryPolicy {
    @Override
    public boolean shouldRetry(Context context) {
      if (context.getInsertErrors().getErrors() != null) {
        for (ErrorProto error : context.getInsertErrors().getErrors()) {
          if (error.getReason() != null && error.getReason().equals("invalidQuery")) {
            return false;
          }
        }
      }
      return true;
    }
  }

  @Test
  public void testCustomRetryPolicy() {
    InsertRetryPolicy policy = new RetryAllExceptInvalidQuery();
    assertTrue(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "unavailable"))));
    assertTrue(policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "invalid"))));
    assertFalse(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "invalidQuery"))));
    assertTrue(
        policy.shouldRetry(new Context(generateErrorAmongMany(5, "timeout", "notImplemented"))));
  }

  private TableDataInsertAllResponse.InsertErrors generateErrorAmongMany(
      int numErrors, String baseReason, String exceptionalReason) {
    // The retry policies are expected to search through the entire list of ErrorProtos to determine
    // whether to retry. Stick the exceptionalReason in a random position to exercise this.
    List<ErrorProto> errorProtos = Lists.newArrayListWithExpectedSize(numErrors);
    int exceptionalPosition = ThreadLocalRandom.current().nextInt(numErrors);
    for (int i = 0; i < numErrors; ++i) {
      ErrorProto error = new ErrorProto();
      error.setReason((i == exceptionalPosition) ? exceptionalReason : baseReason);
      errorProtos.add(error);
    }
    TableDataInsertAllResponse.InsertErrors errors = new TableDataInsertAllResponse.InsertErrors();
    errors.setErrors(errorProtos);
    return errors;
  }
}
